use dope_core::{
    driver::{
        self, retained,
        route::{Identity, table::conn::Capacity},
        schedule::{clock::timer, ready},
    },
    io::{
        event::net::{connect, send::Completion},
        socket,
    },
};
use dope_net::{
    link::{
        event,
        pool::{self, change::opening, recv::input, send},
    },
    wire::open,
};

use crate::{
    app::{creation, env},
    client::{
        app,
        conn::engine::{self, Outbound, Record, Slot, Transition, liveness},
        side::mode::Mode,
    },
};

type State<'d, ID, E, X> =
    liveness::State<'d, ID, <<E as env::Spec>::Transport as dope_net::Transport>::Prepared, X>;

pub(in crate::client) struct Links<'p, 'w, 'd: 'p + 'w, ID: Identity, A, E, X>
where
    A: app::Application<'d, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID>,
{
    pool: Outbound<'p, 'w, 'd, ID, A, E>,
    liveness: State<'d, ID, E, X>,
    primary: Capacity,
}
impl<'p, 'w, 'd: 'p + 'w, ID: Identity, A, E, X> Links<'p, 'w, 'd, ID, A, E, X>
where
    A: app::Application<'d, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID>,
{
    pub(in crate::client) fn capacity(&self) -> Capacity {
        self.primary
    }
    pub(in crate::client) fn pool(&self) -> &Outbound<'p, 'w, 'd, ID, A, E> {
        &self.pool
    }
    pub(in crate::client) fn pool_mut(&mut self) -> &mut Outbound<'p, 'w, 'd, ID, A, E> {
        &mut self.pool
    }
    pub(in crate::client) fn liveness(&self) -> &State<'d, ID, E, X> {
        &self.liveness
    }
    pub(in crate::client) fn liveness_mut(&mut self) -> &mut State<'d, ID, E, X> {
        &mut self.liveness
    }
    pub(in crate::client) fn parts(
        &mut self,
    ) -> (
        &mut Outbound<'p, 'w, 'd, ID, A, E>,
        &mut State<'d, ID, E, X>,
    ) {
        (&mut self.pool, &mut self.liveness)
    }

    pub(in crate::client::conn::engine) fn try_new(
        pool: Outbound<'p, 'w, 'd, ID, A, E>,
        primary: Capacity,
        io: X::Transport<<E::Transport as dope_net::Transport>::Prepared>,
        timer: timer::Timer<'d>,
    ) -> Result<Self, creation::Error> {
        let liveness = liveness::State::try_new(pool.inspect().capacity(), io, timer)?;
        Ok(Self {
            pool,
            liveness,
            primary,
        })
    }
    pub(in crate::client::conn::engine) fn egress(
        &mut self,
        key: pool::Key<'d, ID>,
        target: ready::Target<'d>,
    ) -> Option<liveness::Egress<'_, 'p, 'w, 'd, ID, A, E, X>> {
        let view = self.pool.egress_mut(key)?;
        // SAFETY: this book was allocated from the retained pool's exact slot extent.
        let source = unsafe { engine::raw::reunite(&mut self.liveness, view.connection, key) };
        Some(liveness::Egress {
            flights: view.flights,
            connection: liveness::Change::new(source, target),
            pending: view.pending,
            queue: view.queue,
        })
    }
    pub(in crate::client::conn::engine) fn get<'a>(
        &'a mut self,
        key: pool::Key<'d, ID>,
        target: ready::Target<'d>,
    ) -> Option<Transition<'a, 'd, ID, E, X, &'a mut Slot<'w, 'd, ID, A>>> {
        let slot = self.pool.get_mut(key)?;
        // SAFETY: the native lookup retains this owner's primary slot and its paired book.
        let source = unsafe { engine::raw::reunite(&mut self.liveness, slot, key) };
        Some(liveness::Change::new(source, target))
    }
    pub(in crate::client::conn::engine) fn inspect<'a>(
        &'a mut self,
        key: pool::Key<'d, ID>,
        target: ready::Target<'d>,
    ) -> Option<Transition<'a, 'd, ID, E, X, &'a Slot<'w, 'd, ID, A>>> {
        let slot = self.pool.inspect().get(key)?;
        // SAFETY: inspection retains the same owner's initialized primary slot.
        let source = unsafe { engine::raw::reunite(&mut self.liveness, slot, key) };
        Some(liveness::Change::new(source, target))
    }
    pub(in crate::client::conn::engine) fn sender(
        &mut self,
        key: pool::Key<'d, ID>,
        target: ready::Target<'d>,
    ) -> Option<liveness::Sender<'_, 'p, 'w, 'd, ID, A, E, X>> {
        Some(liveness::Sender {
            native: self.pool.send_slot(key)?,
            liveness: &mut self.liveness,
            key,
            target,
        })
    }
    pub(in crate::client::conn::engine) fn open<P, R>(
        &mut self,
        socket: socket::StreamSpec,
        input: P,
        make: impl FnOnce(
            &P,
        ) -> (
            Record<'d, ID, A>,
            <E::Transport as dope_net::Transport>::Prepared,
        ),
        complete: impl for<'a> FnOnce(Transition<'a, 'd, ID, E, X, &'a Slot<'w, 'd, ID, A>>) -> R,
        target: ready::Target<'d>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> opening::Attempt<'d, ID, P, R, open::Error<'w, A::Wire>> {
        self.pool.open(
            self.primary,
            socket,
            input,
            make,
            |slot| {
                // SAFETY: admission retains a slot of the pool that constructed this book.
                let source = unsafe { engine::raw::reunite(&mut self.liveness, slot, slot.key()) };
                complete(liveness::Change::new(source, target))
            },
            driver,
        )
    }
    pub(in crate::client::conn::engine) fn try_finish<R>(
        &mut self,
        done: connect::Completion<'d>,
        driver: &mut retained::Context<'_, '_, 'd>,
        complete: impl for<'a> FnOnce(Transition<'a, 'd, ID, E, X, &'a Slot<'w, 'd, ID, A>>) -> R,
        target: ready::Target<'d>,
    ) -> Result<event::Connect<'d, ID, R, socket::Addr>, connect::Completion<'d>> {
        self.pool.try_finish(done, driver, |slot| {
            // SAFETY: the original completed primary slot remains in this owner.
            let source = unsafe { engine::raw::reunite(&mut self.liveness, slot, slot.key()) };
            complete(liveness::Change::new(source, target))
        })
    }
    pub(in crate::client::conn::engine) fn try_attach<R>(
        &mut self,
        done: connect::Completion<'d>,
        driver: &mut retained::Context<'_, '_, 'd>,
        complete: impl for<'a> FnOnce(Transition<'a, 'd, ID, E, X, &'a Slot<'w, 'd, ID, A>>) -> R,
        target: ready::Target<'d>,
    ) -> Result<event::Connect<'d, ID, R, ()>, connect::Completion<'d>> {
        self.pool.try_attach(done, driver, |slot| {
            // SAFETY: the original completed primary slot remains in this owner.
            let source = unsafe { engine::raw::reunite(&mut self.liveness, slot, slot.key()) };
            complete(liveness::Change::new(source, target))
        })
    }
    pub(in crate::client::conn::engine) fn settle(
        &mut self,
        done: Completion<'d>,
        driver: &mut retained::Context<'_, '_, 'd>,
        complete: impl for<'a> FnOnce(
            Transition<'a, 'd, ID, E, X, &'a mut Slot<'w, 'd, ID, A>>,
            &event::SendOutcome<'d, ID, ()>,
        ),
        target: ready::Target<'d>,
    ) -> event::SendOutcome<'d, ID, ()>
    where
        <A::Input as input::Mode<A::Wire>>::Ingress: send::Finish<A::Wire>,
    {
        self.pool
            .settle(done)
            .settle_owned(driver, |slot, outcome| {
                let key = slot.key();
                // SAFETY: settlement retains this owner's original native sender and paired book.
                let source = unsafe { engine::raw::reunite(&mut self.liveness, slot, key) };
                complete(liveness::Change::new(source, target), outcome);
            })
    }
}
