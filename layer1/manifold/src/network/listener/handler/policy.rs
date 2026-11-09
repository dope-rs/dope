use std::{convert, pin};

use dope_core::{
    driver::{
        self,
        route::{Identity, table::conn::Capacity},
        schedule,
    },
    io::transfer::Length as _,
};
use dope_net::{
    link::pool::{
        self,
        recv::{delivery, input},
    },
    wire::{
        self, domain,
        receive::{self, Chunk, batch::Items, mode::Disabled, scoped},
    },
};
use handler::send;
use o3::buffer::resident;

use crate::{
    app::{
        env,
        receive::{
            Borrowed, Coupled, Gated, GatedScoped, Mode, Outcome, Owned, Resources, Scoped,
            ingress::{self, Dispatch, batch},
        },
    },
    network::listener::{self, conn, handler},
};

pub(in crate::network::listener) trait Policy<'w, 'd: 'w, ID: Identity, A>:
    Mode<A::Wire> + Dispatch<A::Wire>
where
    A: handler::Application<'d, ID>,
{
    const SEND: bool = <A::Send as send::Policy<'d, ID, A>>::NOTIFY;

    fn send(
        app: pin::Pin<&mut A>,
        scope: conn::Ctx<'_, '_, '_, '_, 'w, 'd, ID, A::Wire, A::Conn, A::Egress>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        <A::Send as send::Policy<'d, ID, A>>::send(app, scope, driver);
    }

    /// Retains no receive credit unless this policy requires it.
    fn retained(_: &A, _: Capacity) -> Result<domain::Retained, crate::ConfigError> {
        Ok(domain::Retained::ZERO)
    }

    /// Reserves no resident bytes unless this policy requires them.
    fn resident(_: &A, _: Capacity) -> Result<domain::Resident, crate::ConfigError> {
        Ok(domain::Resident::ZERO)
    }

    fn receive<'input, E>(
        port: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
        key: pool::Key<'d, ID>,
        input: crate::Incoming<'input, 'w, 'd, A::Wire, Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> crate::Receiving<'w, 'd, A::Wire, Self>
    where
        E: env::Spec<Wire = A::Wire>,
        'w: 'input;
}

fn receive_borrowed<'input, 'w, 'd, ID: Identity, A, E>(
    mut port: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
    key: pool::Key<'d, ID>,
    items: Items<'input, A::Wire>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
) -> ingress::Progress<convert::Infallible>
where
    'd: 'w,
    'w: 'input,
    A: handler::Borrowed<'d, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    let mut this = port.as_mut().project();
    let mut egress = match this.owner.egress_mut(key) {
        Some(egress) => egress,
        None => return ingress::Progress::Done(Outcome::Continue),
    };
    egress
        .connection
        .progress(driver.driver_ref().clock().now());
    batch::drive::<A::Wire, _>(items, |chunk| {
        A::chunk(
            this.app.as_mut(),
            egress.context(turn.reborrow().app()),
            chunk,
            driver,
        )
    })
}

impl<'w, 'd: 'w, ID: Identity, A> Policy<'w, 'd, ID, A> for Borrowed
where
    A: handler::Borrowed<'d, ID> + handler::Application<'d, ID, Input = Borrowed>,
    Borrowed: Mode<A::Wire>
        + input::Mode<A::Wire, Kind = input::Borrowed>
        + Dispatch<A::Wire, Strategy = ingress::Batched>,
{
    fn receive<'input, E>(
        port: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
        key: pool::Key<'d, ID>,
        batch: crate::Incoming<'input, 'w, 'd, A::Wire, Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> ingress::Progress<convert::Infallible>
    where
        E: env::Spec<Wire = A::Wire>,
        'w: 'input,
    {
        receive_borrowed(port, key, batch, turn, driver)
    }
}

impl<'w, 'd: 'w, ID: Identity, A> Policy<'w, 'd, ID, A> for Coupled
where
    A: handler::Borrowed<'d, ID> + handler::Application<'d, ID, Input = Coupled>,
    Coupled: Mode<A::Wire>
        + input::Mode<A::Wire, Kind = input::Coupled>
        + Dispatch<A::Wire, Strategy = ingress::Batched>,
{
    fn receive<'input, E>(
        port: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
        key: pool::Key<'d, ID>,
        batch: crate::Incoming<'input, 'w, 'd, A::Wire, Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> ingress::Progress<convert::Infallible>
    where
        E: env::Spec<Wire = A::Wire>,
        'w: 'input,
    {
        receive_borrowed(port, key, batch, turn, driver)
    }
}

impl<'w, 'd: 'w, ID: Identity, A> Policy<'w, 'd, ID, A> for Gated
where
    A: handler::Borrowed<'d, ID>
        + handler::Gated<'d, ID>
        + handler::Application<'d, ID, Input = Gated>,
    Gated: Mode<A::Wire>
        + input::Mode<A::Wire, Kind = input::Coupled>
        + Dispatch<A::Wire, Strategy = ingress::Batched>,
{
    const SEND: bool = true;

    fn send(
        app: pin::Pin<&mut A>,
        scope: conn::Ctx<'_, '_, '_, '_, 'w, 'd, ID, A::Wire, A::Conn, A::Egress>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        send::gated(app, scope, driver);
    }

    fn receive<'input, E>(
        port: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
        key: pool::Key<'d, ID>,
        batch: crate::Incoming<'input, 'w, 'd, A::Wire, Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> ingress::Progress<convert::Infallible>
    where
        E: env::Spec<Wire = A::Wire>,
        'w: 'input,
    {
        receive_borrowed(port, key, batch, turn, driver)
    }
}

fn receive_scoped<'e, 'w, 'd: 'w, ID: Identity, A, E>(
    mut port: pin::Pin<&mut listener::Listener<'e, '_, '_, 'w, 'd, ID, A, E>>,
    key: pool::Key<'d, ID>,
    mut chunk: Chunk<'w, 'd, A::Wire, Disabled>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
) -> ingress::Progress<Chunk<'w, 'd, A::Wire, Disabled>>
where
    A: handler::Scoped<'w, 'd, ID>,
    A::Input: Resources<Authority<'d> = resident::Budget<'d>>,
    E: env::Spec<Wire = A::Wire>,
{
    let mut this = port.as_mut().project();
    let done = match this.owner.egress_mut(key) {
        Some(mut egress) => {
            egress
                .connection
                .progress(driver.driver_ref().clock().now());
            A::scoped_chunk(
                this.app.as_mut(),
                egress.context(turn.app()),
                scoped::Input::new(&mut chunk, this.receive_authority),
                driver,
            )
        }
        None => Outcome::Continue,
    };
    match done {
        Outcome::Continue if chunk.is_empty() => ingress::Progress::Done(Outcome::Continue),
        Outcome::Continue => ingress::Progress::Pending(chunk),
        done @ (Outcome::CloseAfter | Outcome::Failed(_)) => ingress::Progress::Done(done),
    }
}

impl<'w, 'd: 'w, ID: Identity, A, const GATED: bool> Policy<'w, 'd, ID, A> for Scoped<GATED>
where
    A: handler::Scoped<'w, 'd, ID> + handler::Application<'d, ID, Input = Scoped<GATED>>,
    Scoped<GATED>: input::Mode<A::Wire, Kind = input::Scoped>
        + Dispatch<A::Wire, Strategy = ingress::Scope>
        + Resources<Authority<'d> = resident::Budget<'d>>,
    <Scoped<GATED> as input::Mode<A::Wire>>::Kind: input::Replay<A::Wire>,
{
    fn resident(app: &A, count: Capacity) -> Result<domain::Resident, crate::ConfigError> {
        app.retention().capacity(count)
    }

    fn receive<'input, E>(
        port: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
        key: pool::Key<'d, ID>,
        chunk: crate::Incoming<'input, 'w, 'd, A::Wire, Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> ingress::Progress<Chunk<'w, 'd, A::Wire, Disabled>>
    where
        E: env::Spec<Wire = A::Wire>,
        'w: 'input,
    {
        receive_scoped(port, key, chunk, turn, driver)
    }
}

impl<'w, 'd: 'w, ID: Identity, A> Policy<'w, 'd, ID, A> for GatedScoped
where
    A: handler::Scoped<'w, 'd, ID>
        + handler::Gated<'d, ID>
        + handler::Application<'d, ID, Input = GatedScoped>,
    GatedScoped:
        input::Mode<A::Wire, Kind = input::Scoped> + Dispatch<A::Wire, Strategy = ingress::Scope>,
    <GatedScoped as input::Mode<A::Wire>>::Kind: input::Replay<A::Wire>,
{
    const SEND: bool = true;

    fn send(
        app: pin::Pin<&mut A>,
        scope: conn::Ctx<'_, '_, '_, '_, 'w, 'd, ID, A::Wire, A::Conn, A::Egress>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        send::gated(app, scope, driver);
    }

    fn resident(app: &A, count: Capacity) -> Result<domain::Resident, crate::ConfigError> {
        app.retention().capacity(count)
    }

    fn receive<'input, E>(
        port: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
        key: pool::Key<'d, ID>,
        chunk: crate::Incoming<'input, 'w, 'd, A::Wire, Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> ingress::Progress<Chunk<'w, 'd, A::Wire, Disabled>>
    where
        E: env::Spec<Wire = A::Wire>,
        'w: 'input,
    {
        receive_scoped(port, key, chunk, turn, driver)
    }
}

impl<'w, 'd: 'w, ID: Identity, A> Policy<'w, 'd, ID, A> for Owned
where
    A: handler::Owned<'w, 'd, ID>,
    Owned: Mode<A::Wire>
        + input::Mode<A::Wire, Kind = input::Retained>
        + Dispatch<A::Wire, Strategy = ingress::Retain>,
    <<A::Wire as wire::Spec>::Receive as receive::Policy<A::Wire>>::Credit:
        delivery::Ownership<A::Wire>,
{
    fn retained(app: &A, count: Capacity) -> Result<domain::Retained, crate::ConfigError> {
        app.retention().capacity(count)
    }

    fn receive<'input, E>(
        mut port: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
        key: pool::Key<'d, ID>,
        chunk: crate::Incoming<'input, 'w, 'd, A::Wire, Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> ingress::Progress<convert::Infallible>
    where
        E: env::Spec<Wire = A::Wire>,
        'w: 'input,
    {
        let mut this = port.as_mut().project();
        let done = match this.owner.egress_mut(key) {
            Some(mut egress) => {
                egress
                    .connection
                    .progress(driver.driver_ref().clock().now());
                A::chunk(
                    this.app.as_mut(),
                    egress.context(turn.app()),
                    chunk.0,
                    driver,
                )
            }
            None => Outcome::Continue,
        };
        ingress::Progress::Done(done)
    }
}
