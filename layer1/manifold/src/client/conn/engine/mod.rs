use std::{ops::ControlFlow, pin};

use dope_core::{
    driver::{
        self,
        route::{Identity, indices, table},
        schedule::{self, maint, ready},
    },
    io,
};
use dope_net::link::{
    event::ParkError,
    pool::{
        self, pending,
        recv::{input, reserve::Choice},
    },
};
use dope_runtime::executor;
use o3::base::time::Positive;

use self::{
    drive::{cycle, phase},
    ops::{close::ClosePhase, connect::Connect},
    shutdown::state,
    transfer::{flush::Flush, send, wakes},
};
use crate::{
    app::{
        dispatch::{self, event},
        env,
        receive::ingress::{self, Dispatch},
    },
    client::{
        self, app, attempt,
        close::{Directive, Reason},
        conn::{self, Id},
        side::{self, complete::Complete, mode::Mode, registry},
    },
};

mod drive;
pub(in crate::client) use drive::cycle::Attempts;
pub(in crate::client) mod liveness;
pub(in crate::client) mod ops;
pub mod retain;
pub(crate) mod shutdown;
mod startup;
pub use startup::Startup;
pub(in crate::client) mod transfer;

/// Primary connector state retains the exact application and attempt identity.
type Record<'d, ID, A> =
    conn::State<'d, <A as app::Application<'d, ID>>::Conn, side::Primary<'d, ID>>;
type Outbound<'p, 'w, 'd, ID, A, E> = pool::Outbound<
    'p,
    'w,
    'd,
    ID,
    <E as env::Spec>::Transport,
    <A as app::Application<'d, ID>>::Wire,
    Record<'d, ID, A>,
    <A as app::Application<'d, ID>>::Input,
    <A as app::Application<'d, ID>>::Send,
    { client::IOV_CAP },
>;
type Slot<'w, 'd, ID, A> = conn::Slot<
    'w,
    'd,
    ID,
    <A as app::Application<'d, ID>>::Wire,
    <A as app::Application<'d, ID>>::Conn,
    side::Primary<'d, ID>,
>;

/// A connection change retains the environment's exact prepared transport authority.
type Transition<'a, 'd, ID, E, X, S> = liveness::Change<
    'a,
    'd,
    ID,
    <<E as env::Spec>::Transport as dope_net::Transport>::Prepared,
    X,
    S,
>;

#[pin_project::pin_project(!Unpin)]
/// A connector retains its route identity for the complete driver domain.
pub struct Machine<'p, 'w, 'd: 'p + 'w, ID: Identity + 'd, A, S, E, X = side::Disabled>
where
    A: app::Application<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    X: Mode<'d, ID>,
{
    pub(in crate::client) links: liveness::Links<'p, 'w, 'd, ID, A, E, X>,
    pub(crate) app: A,
    pub(in crate::client) controller: S,
    pub(in crate::client) side: X,
    pub(in crate::client) shutdown: state::State,
    connection_lifetime: Option<Positive>,
    #[pin]
    drive: cycle::Cycle<'d, ID, phase::Phase<S::Mode, X>>,
}

impl<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E> executor::raw::Finishes<'d, Self>
    for Machine<'p, 'w, 'd, ID, A, S, E, side::Disabled>
where
    A: app::Lifecycle<'w, 'd, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    fn call<'app, 'finish, 'owner, 'epoch>(
        finish: executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self>,
    ) -> executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self> {
        let (mut owner, install, proof) = finish.into_parts();
        let this = owner.as_mut().project();
        this.links
            .pool_mut()
            .finish(proof, |payload, token| this.app.cancel(payload, token));
        executor::raw::Finish::new(owner, install, proof)
    }
}

impl<'r, 'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, I> executor::raw::Finishes<'d, Self>
    for Machine<'p, 'w, 'd, ID, A, S, E, side::Enabled<&'r registry::Registry<'d, I, ID>>>
where
    A: app::Lifecycle<'w, 'd, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    I: registry::Intent<'d, ID>,
{
    fn call<'app, 'finish, 'owner, 'epoch>(
        finish: executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self>,
    ) -> executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self> {
        let (mut owner, install, proof) = finish.into_parts();
        let this = owner.as_mut().project();
        if proof.is_forced() {
            this.side.registry.close_intake();
            while let registry::Rejection::Settled = this.side.registry.reject() {}
            this.links.liveness_mut().finish(proof, |ticket| {
                this.side
                    .registry
                    .complete(ticket, Err(side::Error::ForcedShutdown));
            });
        }
        this.links
            .pool_mut()
            .finish(proof, |payload, token| this.app.cancel(payload, token));
        executor::raw::Finish::new(owner, install, proof)
    }
}

impl<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> Machine<'p, 'w, 'd, ID, A, S, E, X>
where
    A: app::Application<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    X: Mode<'d, ID>,
{
    fn flush_recv(
        self: pin::Pin<&mut Self>,
        work: maint::Work<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let this = self.project();
        let Err(failure) = this.links.pool_mut().recv().flush(work, driver) else {
            return;
        };
        let Some((slot, handle)) = pending::Mut::of(this.links.pool_mut()).get(failure.key) else {
            return;
        };
        let error = this
            .controller
            .rejected(slot.state.owner.attempt(), failure.cause);
        slot.state
            .closing
            .apply(Directive::from_receive(ParkError::Submit(error)));
        handle.wake();
    }

    /// Borrows the protocol application from this address-stable engine.
    pub fn application(self: pin::Pin<&Self>) -> &A {
        self.project_ref().app
    }

    pub(crate) fn session_admission<'a>(
        self: pin::Pin<&'a mut Self>,
        current: impl Fn(&S, attempt::Id<'d, ID>) -> bool + 'a,
    ) -> (&'a mut A, impl Fn(Id<'d, ID>) -> bool + 'a) {
        let this = self.project();
        let slots = this.links.pool().inspect();
        let control = &*this.controller;
        let open = this.shutdown.is_open();
        (this.app, move |id: Id<'d, ID>| {
            if !open {
                return false;
            }
            let Some(slot) = slots.get(id.key()) else {
                return false;
            };
            slot.state.is_attached()
                && slot.state.closing.status() == conn::CloseStatus::Open
                && current(control, slot.state.owner.attempt())
        })
    }

    pub(crate) fn control(self: pin::Pin<&Self>) -> &S {
        self.project_ref().controller
    }

    pub(crate) fn control_mut(self: pin::Pin<&mut Self>) -> &mut S {
        self.project().controller
    }

    pub(crate) fn is_open(self: pin::Pin<&Self>) -> bool {
        self.project_ref().shutdown.is_open()
    }

    pub(crate) fn capacity(self: pin::Pin<&Self>) -> table::Capacity {
        self.project_ref().links.pool().inspect().capacity()
    }

    pub(crate) fn wake_control(self: pin::Pin<&Self>) {
        self.project_ref().drive.activate();
    }

    pub(crate) fn retire<'turn>(
        self: pin::Pin<&mut Self>,
        lane: indices::SlotIndex,
        permit: maint::Permit<'turn, 'd>,
        retired: impl FnOnce(&mut S, attempt::Id<'d, ID>) -> bool,
    ) where
        A: app::Receive<'d, ID>,
    {
        permit.run(|| {
            let this = self.project();
            let Some(key) = this.links.pool().inspect().key_at(lane) else {
                return;
            };
            let Some((slot, handle)) = pending::Mut::of(this.links.pool_mut()).get(key) else {
                return;
            };
            if !retired(this.controller, slot.state.owner.attempt()) {
                if this.controller.is_pending() {
                    this.drive.activate();
                }
                return;
            }
            if slot.state.closing.withdraw() {
                let fence = match slot.state.conn_mut() {
                    Some(conn) => this.app.withdraw(Id::from_key(key), conn),
                    None => app::Fence::Ready,
                };
                if matches!(fence, app::Fence::Ready) {
                    slot.state
                        .closing
                        .apply(Directive::reconnect(Reason::EndpointRetired));
                }
            }
            handle.wake();
        });
    }

    pub(crate) fn conn_mut(self: pin::Pin<&mut Self>, conn: Id<'d, ID>) -> Option<&mut A::Conn> {
        let this = self.project();
        let slot = this.links.pool_mut().get_mut(conn.key())?;
        slot.state.conn_mut()
    }

    pub(crate) fn wake_conn(self: pin::Pin<&mut Self>, conn: Id<'d, ID>) -> app::Wake {
        let this = self.project();
        let Some(slot) = this.links.pool_mut().get_mut(conn.key()) else {
            return app::Wake::Idle;
        };
        slot.io().wake_target().wake();
        app::Wake::Ready
    }
}

impl<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> Machine<'p, 'w, 'd, ID, A, S, E, X>
where
    A: app::Application<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    X: Mode<'d, ID>,
{
    pub(in crate::client) fn backoff(self: pin::Pin<&Self>) -> ready::Target<'d> {
        self.get_ref().drive.target()
    }
}

impl<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> event::Turn<'d>
    for Machine<'p, 'w, 'd, ID, A, S, E, X>
where
    A: app::Receive<'d, ID> + app::Lifecycle<'w, 'd, ID> + app::Scheduling<'d, ID>,
    A::Input: app::Policy<'w, 'd, ID, A> + Dispatch<A::Wire>,
    A::Requests: send::Policy<'w, 'd, ID, A>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    X: Mode<'d, ID>
        + Complete<'w, 'd, ID, A, S, E>
        + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    A::Continuation: Flush<'p, 'w, 'd, ID, A, S, E, X>,
    <A::Input as input::Mode<A::Wire>>::Reserve:
        Choice<A::Wire, <A::Input as input::Mode<A::Wire>>::Ingress>,
    Self: ClosePhase<'d, ID, A, S, E>
        + ingress::Policy<'p, 'w, 'd, ID, Input = A::Input, Wire = A::Wire>
        + wakes::Completion<'d>
        + Connect<'d, ID, A, S, E>,
{
    type Access = dispatch::raw::Retained;

    fn dispatch<'app, 'driver, 'turn>(
        call: executor::raw::Call<
            'app,
            Self,
            (),
            (
                io::Event<'d>,
                schedule::Turn<'turn, 'd>,
                dispatch::raw::Context<'driver, 'd, Self::Access>,
            ),
        >,
    ) -> ControlFlow<io::Event<'d>> {
        use io::event::Kind;

        let (mut this, (), (event, turn, mut driver)) = call.into_parts();
        match event.into_kind() {
            Kind::Recv(done) => {
                if let ControlFlow::Break(done) =
                    ingress::Policy::dispatch(this.as_mut(), done, turn.reborrow(), &mut driver)
                {
                    return ControlFlow::Break(io::Event::from(done));
                }
                Complete::flush(this, turn, &mut driver);
                return ControlFlow::Continue(());
            }
            Kind::Send(done) => {
                if let Err(done) = Complete::send(this.as_mut(), done, turn.reborrow(), &mut driver)
                {
                    return ControlFlow::Break(Kind::Send(done).into());
                }
            }
            Kind::Socket(done) => {
                Complete::socket(this.as_mut(), done, turn.reborrow(), &mut driver);
            }
            Kind::Connect(done) => {
                if let Err(done) =
                    Complete::connect(this.as_mut(), done, turn.reborrow(), &mut driver)
                {
                    return ControlFlow::Break(Kind::Connect(done).into());
                }
            }
            event => return ControlFlow::Break(event.into()),
        }
        this.as_mut()
            .flush_recv(turn.reborrow().maint(), &mut driver);
        Complete::flush(this, turn, &mut driver);
        ControlFlow::Continue(())
    }
}

mod raw;
