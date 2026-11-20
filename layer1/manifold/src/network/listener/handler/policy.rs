use std::pin;

use dope_core::driver::{
    self,
    route::{Identity, table::conn::Capacity},
    schedule,
};
use dope_net::{
    link::pool::recv::{delivery, input},
    wire::{
        self, domain,
        receive::{self, Chunk, batch::Items, mode::Disabled, scoped},
    },
};
use handler::send;
use o3::buffer::resident;

use crate::{
    app::receive::{
        Borrowed, Coupled, Gated, GatedScoped, Mode, Outcome, Owned, Resources, Scoped,
        ingress::{self, Dispatch, batch},
    },
    network::listener::{conn, handler, writer},
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

    fn receive<'input>(
        app: pin::Pin<&mut A>,
        egress: &mut writer::Egress<'_, '_, '_, '_, 'w, 'd, ID, A>,
        authority: &mut <Self as Resources>::Authority<'d>,
        input: crate::Incoming<'input, 'w, 'd, A::Wire, Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> crate::Receiving<A::Wire, Self>
    where
        'w: 'input;
}

fn receive_borrowed<'input, 'w, 'd, ID: Identity, A>(
    mut app: pin::Pin<&mut A>,
    egress: &mut writer::Egress<'_, '_, '_, '_, 'w, 'd, ID, A>,
    items: Items<'input, A::Wire>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
) -> Outcome
where
    'd: 'w,
    'w: 'input,
    A: handler::Borrowed<'d, ID>,
{
    egress
        .connection
        .progress(driver.driver_ref().clock().now());
    batch::drive::<A::Wire, _, _>(items, egress, |egress, chunk| {
        let outcome = A::chunk(
            app.as_mut(),
            egress.context(turn.reborrow().app()),
            chunk,
            driver,
        );
        (Some(egress), outcome)
    }).1
}

impl<'w, 'd: 'w, ID: Identity, A> Policy<'w, 'd, ID, A> for Borrowed
where
    A: handler::Borrowed<'d, ID> + handler::Application<'d, ID, Input = Borrowed>,
    Borrowed: Mode<A::Wire>
        + input::Mode<A::Wire, Kind = input::Borrowed>
        + Dispatch<A::Wire, Strategy = ingress::Batched>,
{
    fn receive<'input>(
        app: pin::Pin<&mut A>,
        egress: &mut writer::Egress<'_, '_, '_, '_, 'w, 'd, ID, A>,
        _: &mut <Self as Resources>::Authority<'d>,
        batch: crate::Incoming<'input, 'w, 'd, A::Wire, Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Outcome
    where
        'w: 'input,
    {
        receive_borrowed(app, egress, batch, turn, driver)
    }
}

impl<'w, 'd: 'w, ID: Identity, A> Policy<'w, 'd, ID, A> for Coupled
where
    A: handler::Borrowed<'d, ID> + handler::Application<'d, ID, Input = Coupled>,
    Coupled: Mode<A::Wire>
        + input::Mode<A::Wire, Kind = input::Coupled>
        + Dispatch<A::Wire, Strategy = ingress::Batched>,
{
    fn receive<'input>(
        app: pin::Pin<&mut A>,
        egress: &mut writer::Egress<'_, '_, '_, '_, 'w, 'd, ID, A>,
        _: &mut <Self as Resources>::Authority<'d>,
        batch: crate::Incoming<'input, 'w, 'd, A::Wire, Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Outcome
    where
        'w: 'input,
    {
        receive_borrowed(app, egress, batch, turn, driver)
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

    fn receive<'input>(
        app: pin::Pin<&mut A>,
        egress: &mut writer::Egress<'_, '_, '_, '_, 'w, 'd, ID, A>,
        _: &mut <Self as Resources>::Authority<'d>,
        batch: crate::Incoming<'input, 'w, 'd, A::Wire, Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Outcome
    where
        'w: 'input,
    {
        receive_borrowed(app, egress, batch, turn, driver)
    }
}

fn receive_scoped<'w, 'd: 'w, ID: Identity, A>(
    mut app: pin::Pin<&mut A>,
    egress: &mut writer::Egress<'_, '_, '_, '_, 'w, 'd, ID, A>,
    authority: &mut resident::Budget<'d>,
    chunk: &mut Chunk<'w, 'd, A::Wire, Disabled>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
) -> Option<Outcome>
where
    A: handler::Scoped<'w, 'd, ID>,
    A::Input: Resources<Authority<'d> = resident::Budget<'d>>,
{
    egress.connection.progress(driver.driver_ref().clock().now());
    match scoped::with(chunk, authority, |input| {
        A::scoped_chunk(app.as_mut(), egress.context(turn.app()), input, driver)
    }) {
        Ok(done) => done,
        Err(cause) => Some(Outcome::Failed(crate::app::receive::Failure::Input(cause))),
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

    fn receive<'input>(
        app: pin::Pin<&mut A>,
        egress: &mut writer::Egress<'_, '_, '_, '_, 'w, 'd, ID, A>,
        authority: &mut <Self as Resources>::Authority<'d>,
        chunk: crate::Incoming<'input, 'w, 'd, A::Wire, Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Option<Outcome>
    where
        'w: 'input,
    {
        receive_scoped(app, egress, authority, chunk, turn, driver)
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

    fn receive<'input>(
        app: pin::Pin<&mut A>,
        egress: &mut writer::Egress<'_, '_, '_, '_, 'w, 'd, ID, A>,
        authority: &mut <Self as Resources>::Authority<'d>,
        chunk: crate::Incoming<'input, 'w, 'd, A::Wire, Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Option<Outcome>
    where
        'w: 'input,
    {
        receive_scoped(app, egress, authority, chunk, turn, driver)
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

    fn receive<'input>(
        mut app: pin::Pin<&mut A>,
        egress: &mut writer::Egress<'_, '_, '_, '_, 'w, 'd, ID, A>,
        _: &mut <Self as Resources>::Authority<'d>,
        chunk: crate::Incoming<'input, 'w, 'd, A::Wire, Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Outcome
    where
        'w: 'input,
    {
        egress.connection.progress(driver.driver_ref().clock().now());
        A::chunk(app.as_mut(), egress.context(turn.app()), chunk.0, driver)
    }
}
