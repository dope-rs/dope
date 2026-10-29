pub mod config;
pub mod conn;
mod control;
pub mod handler;
pub mod raw;
mod runtime;
pub mod topology;
mod writer;

use std::{net, ops::ControlFlow, pin};

pub use control::Control;
use dope_core::{
    driver,
    driver::{
        result, retained,
        route::Identity,
        schedule::{self, maint},
    },
    io::{self, event::net::accept},
};
use dope_net::{
    Listen,
    link::{
        egress,
        pool::{pending, send},
    },
    wire,
};
use dope_runtime::{
    executor,
    random::{self, HashState},
};
use handler::Runtime;
use o3::{base::cell::region, depot::capacity};
pub use runtime::install::Install;
pub use runtime::prepared::Prepared;
use runtime::{
    accept::{Accept, phase},
    control::receive_failed,
};
use wire::domain;
use writer::phase::flush_dirty;

use crate::app::{creation, dispatch, dispatch::event, env, receive, receive::ingress};

pub(in crate::network::listener) const IOV_CAP: usize = ingress::IOV_CAP;

const HASH_NAMESPACE: random::Namespace = random::Namespace::new(*b"\0\0accept");
pub enum Domain {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcceptError {
    Submit(result::SubmitError),
    Completion(accept::Failure),
    IdentityExhausted,
}

impl Domain {
    pub const DEFAULT: random::Domain = HASH_NAMESPACE.fixed();

    pub fn for_listener(id: u8) -> random::Domain {
        HASH_NAMESPACE.instance(u64::from(id))
    }
}

impl<'n, 'p, 's, 'w, 'd: 'w, ID: Identity, A, E> event::Turn<'d>
    for Listener<'n, 'p, 's, 'w, 'd, ID, A, E>
where
    A: handler::Application<'d, ID>,
    A: Runtime<'w, 'd, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    type Access = dispatch::raw::Retained;

    fn dispatch<'app, 'driver, 'turn, 'root>(
        call: executor::raw::Call<
            'app,
            Self,
            (),
            (
                io::Event<'d>,
                schedule::Turn<'turn, 'd>,
                dispatch::raw::Context<'driver, 'root, 'd, Self::Access>,
            ),
        >,
    ) -> ControlFlow<io::Event<'d>>
    where
        'd: 'root,
    {
        use io::event::Kind;

        let (this, (), (ev, turn, mut driver)) = call.into_parts();
        let mut this = this;
        match ev.into_kind() {
            Kind::Recv(done) => {
                if let ControlFlow::Break(done) = <A as Runtime<'w, 'd, ID>>::dispatch(
                    this.as_mut(),
                    done,
                    turn.reborrow(),
                    &mut driver,
                ) {
                    return ControlFlow::Break(io::Event::from(done));
                }
                this.as_mut()
                    .arm_accept(turn.reborrow().maint(), &mut driver);
                flush_dirty(this, turn.reborrow(), &mut driver);
                return ControlFlow::Continue(());
            }
            Kind::Send(done) => {
                use writer::phase::send::pump_send;

                pump_send(this.as_mut(), done, turn.reborrow(), &mut driver)
            }
            Kind::Accept(done) => {
                phase::accept(this.as_mut(), done, turn.reborrow(), &mut driver);
            }
            Kind::Tuning(done) => {
                if let Err(done) = phase::tune(this.as_mut(), done, turn.reborrow(), &mut driver) {
                    return ControlFlow::Break(Kind::Tuning(done).into());
                }
            }
            kind @ (Kind::RetiredRecv(_)
            | Kind::Socket(_)
            | Kind::Connect(_)
            | Kind::Open(_)
            | Kind::Write(_)
            | Kind::Sync(_)
            | Kind::Shutdown) => return ControlFlow::Break(kind.into()),
        }
        this.as_mut()
            .arm_accept(turn.reborrow().maint(), &mut driver);
        this.as_mut().flush_recv(turn.reborrow(), &mut driver);
        flush_dirty(this, turn.reborrow(), &mut driver);
        ControlFlow::Continue(())
    }
}

#[pin_project::pin_project(!Unpin)]
pub struct Listener<'n, 'p, 's, 'w, 'd: 'w, ID: Identity + 'd, A, E>
where
    A: handler::Application<'d, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    owner: writer::Owner<'n, 'p, 's, 'w, 'd, ID, E::Transport, A>,
    #[pin]
    app: A,
    receive_authority: <A::Input as receive::Resources>::Authority<'d>,
    #[pin]
    accept: Accept<'d, ID>,
    bound_addr: net::SocketAddr,
    schedule: runtime::Schedule,
}

impl<'n, 'p, 's, 'w, 'd: 'w, ID: Identity, A, E> executor::raw::Finishes<'d, Self>
    for Listener<'n, 'p, 's, 'w, 'd, ID, A, E>
where
    A: handler::Application<'d, ID>,
    A: Runtime<'w, 'd, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    fn call<'app, 'finish, 'owner, 'epoch>(
        finish: executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self>,
    ) -> executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self> {
        let (mut owner, install, proof) = finish.into_parts();
        let mut this = owner.as_mut().project();
        this.accept.finish(proof);
        let mut app = this.app.as_mut();
        this.owner.pool_mut().finish_keyed(proof, |key, payload| {
            writer::release_payload(app.as_mut(), key, payload, handler::Disposition::Closed);
        });
        executor::raw::Finish::new(owner, install, proof)
    }
}

impl<'n, 'p, 's, 'w, 'd: 'w, ID: Identity, A, E> Listener<'n, 'p, 's, 'w, 'd, ID, A, E>
where
    A: handler::Application<'d, ID>,
    A: Runtime<'w, 'd, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    pub fn adopt_in<C, O>(
        app: A,
        config: config::Prepared<E::Transport>,
        bound: <E::Transport as Listen>::BoundListener,
        hash: HashState<'d>,
        owner: &mut C,
        context: impl for<'a> Fn(&'a mut C) -> driver::Context<'a, 'd>,
        build: impl for<'pool, 'gate, 'store> FnOnce(
            Listener<'pool, 'gate, 'store, 'w, 'd, ID, A, E>,
            &mut C,
        ) -> O,
    ) -> Result<O, creation::Error>
    where
        E::Transport: Listen,
        <A::Wire as wire::Spec>::Bootstrap: domain::Init<A::Wire>,
        <A::Wire as wire::Spec>::Binding<'w, ID>: Default,
    {
        Install::<ID, E, _>::new(config, domain::Local::new(Default::default()))
            .with_bound(Some(bound))
            .with_driver(app, hash, owner, context, build)
    }
}

impl<'n, 'p, 's, 'w, 'd: 'w, ID: Identity, A, E> Listener<'n, 'p, 's, 'w, 'd, ID, A, E>
where
    A: handler::Application<'d, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    pub const fn local_addr(&self) -> net::SocketAddr {
        self.bound_addr
    }

    /// The terminal failure that stopped accept admission.
    /// Native completion failures also reach the scope's reclamation observer.
    pub fn accept_error(&self) -> Option<AcceptError> {
        self.accept.failure()
    }

    fn arm_accept(
        mut self: pin::Pin<&mut Self>,
        work: maint::Work<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let sent = {
            let fields = self.as_mut().project();
            let avail = fields.owner.pool().inspect().available();
            fields.accept.arm(driver, avail, work)
        };
        if let Err(error) = sent {
            self.as_mut()
                .project()
                .accept
                .failed(AcceptError::Submit(error));
            self.seal(driver.driver());
        }
    }

    fn flush_recv(
        mut self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let received = self
            .as_mut()
            .project()
            .owner
            .pool_mut()
            .recv()
            .flush(turn.reborrow().maint(), driver);
        if let Err(failure) = received {
            receive_failed(self, failure, turn, driver);
        }
    }

    pub fn handler(&self) -> &A {
        &self.app
    }

    pub fn capacity(&self) -> capacity::Count {
        self.owner.pool().inspect().capacity().count()
    }

    pub fn len(&self) -> capacity::Count {
        self.owner.pool().inspect().len()
    }

    /// Monotonically stops new accepts while preserving every existing slot.
    /// Existing slots are marked close-after in bounded maintenance steps, so
    /// the application close gate remains authoritative.
    pub fn seal(self: pin::Pin<&mut Self>, driver: &mut driver::Context<'_, 'd>) {
        let this = self.project();
        if this.schedule.is_open() {
            this.schedule.seal();
            A::seal(this.app);
            this.accept.stop_accept(driver);
        }
    }

    pub fn mark_send(
        &self,
        region: &mut region::Token<region::Identity<'d>>,
        id: conn::Id<'d, ID>,
        bytes: egress::NonEmpty<A::Egress>,
    ) -> Result<(), send::Rejected<A::Egress>> {
        self.owner.pool().try_right(region, id.key(), bytes)
    }

    pub fn close(&self, id: conn::Id<'d, ID>) {
        pending::Pending::of(self.owner.pool()).close(id.key());
    }
}
