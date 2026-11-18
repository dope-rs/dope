use std::ops;

use data::compose;
use dope_core::{
    driver::{
        self,
        life::finish,
        retained::flight,
        route::{self, Identity, lease, table::conn},
        schedule::maint,
    },
    io::{
        event::net::tuning,
        fd::handles,
        socket::option,
    },
};
use o3::{base::cell::region, buffer::storage::Shared};

use self::{
    change::{accept, drain},
    recv::input::{self, Dispatch},
};
use crate::{
    link::{
        egress::{self, data, reserve::demand},
        setup,
        slot::{transmit, types},
    },
    wire::{
        self, domain,
        open::{self, Runtime as _},
    },
};

pub mod change;
mod entry;
mod inspect;
mod key;
mod outbound;
pub mod pending;
pub mod raw;
pub mod recv;
pub mod send;
pub use entry::{Entry, Missing};
pub use inspect::Inspect;
pub use key::Key;
pub(in crate::link) use key::Keyspace;
pub use outbound::Outbound;
pub(in crate::link) use outbound::Stored;

type Sending<'a, 'p, 'w, 'd, ID, T, W, S, M, B, const IOV: usize, D> = Option<(
    transmit::Sender<'a, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
    bool,
)>;

pub enum Released<S, B> {
    Payload(B),
    State(S),
}

/// Connection storage which exclusively owns the route of every retained target.
pub struct Connections<
    'p,
    'w,
    'd,
    ID: Identity + 'w,
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    S,
    M: input::Mode<W>,
    B: data::raw::Payload<'d> = Shared,
    const IOV: usize = 32,
    D: demand::Mode = demand::Fallible,
> where
    'd: 'w,
{
    keys: lease::Owner<'d, ID, Keyspace<'d, ID>>,
    prepared: change::raw::Prepared<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
}

impl<
    'p,
    'w,
    'd,
    ID: Identity + 'w,
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    S,
    M: input::Mode<W>,
    B: data::raw::Payload<'d>,
    const IOV: usize,
    D: demand::Mode,
> Drop for Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>
where
    'd: 'w,
{
    fn drop(&mut self) {
        // Preserve issued generations before the route owner releases its claim.
        self.keys.advance_to(self.prepared.slab.next_generation());
    }
}

pub struct Pool<
    'p,
    'w,
    'd,
    ID: Identity + 'w,
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    S,
    M: input::Mode<W>,
    B: data::raw::Payload<'d> = Shared,
    const IOV: usize = 32,
    D: demand::Mode = demand::Fallible,
> where
    'd: 'w,
{
    storage: Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
}

/// Exclusive connection and egress projection backed by one pool borrow.
pub struct EgressMut<
    'a,
    'p,
    'w,
    'd,
    ID: Identity + 'w,
    W: wire::Spec + 'w,
    S,
    B: data::raw::Payload<'d>,
    const IOV: usize,
    D: demand::Mode = demand::Fallible,
> {
    pub flights: &'a flight::Slots<'d, ID>,
    pub connection: &'a mut types::Connection<'w, 'd, ID, W, S>,
    pub pending: pending::Handle<'a, 'd, ID>,
    pub queue: egress::Queue<'a, 'p, 'd, IOV, B, D>,
}

impl<
    'p,
    'w,
    'd,
    ID: Identity + 'w,
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    S,
    M,
    B: data::raw::Payload<'d>,
    const IOV: usize,
    D: demand::Mode,
> ops::Deref for Pool<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>
where
    M: input::Mode<W>,
    'd: 'w,
{
    type Target = Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>;

    fn deref(&self) -> &Self::Target {
        &self.storage
    }
}

impl<
    'p,
    'w,
    'd,
    ID: Identity + 'w,
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    S,
    M,
    B: data::raw::Payload<'d>,
    const IOV: usize,
    D: demand::Mode,
> ops::DerefMut for Pool<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>
where
    M: input::Mode<W>,
    'd: 'w,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.storage
    }
}

impl<
    'p,
    'w,
    'd,
    ID: Identity + 'w,
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    S,
    M,
    B: data::raw::Payload<'d>,
    const IOV: usize,
    D: demand::Mode,
> Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>
where
    M: input::Mode<W>,
    'd: 'w,
{
    pub fn finish(
        &mut self,
        finish: &mut finish::Finish<'_, 'd>,
        free: impl FnMut(B, &mut region::Token<region::Identity<'d, 1>>),
    ) where
        M::Ingress: drain::Policy<W>,
    {
        <M::Ingress as drain::Policy<W>>::finish(self, finish, free);
    }

    pub fn finish_keyed<R>(
        &mut self,
        finish: &mut finish::Finish<'_, 'd>,
        retire: impl FnMut(&mut S) -> R,
        free: impl FnMut(Key<'d, ID>, Released<R, B>),
    ) where
        M::Ingress: drain::Policy<W>,
    {
        <M::Ingress as drain::Policy<W>>::finish_keyed(self, finish, retire, free);
    }

    pub fn recv(&mut self) -> recv::Receiver<'_, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D> {
        recv::Receiver::new(self)
    }

    pub fn reconfigure(&mut self, config: <W::Runtime<'w, 'd, ID> as domain::Reconfigure>::Config)
    where
        W::Runtime<'w, 'd, ID>: domain::Reconfigure,
    {
        domain::Reconfigure::reconfigure(&mut self.prepared.runtime, config);
    }

    /// Spends one maintenance permit per selection, including stale activations.
    /// Call before parking and report `inspect().has_credit()` as runnable,
    /// including during shutdown.
    pub fn credit_step(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        work: maint::Work<'_, 'd>,
    ) {
        self.prepared.egress.maintain(token, work);
    }

    pub fn suspend(&mut self, key: Key<'d, ID>) {
        if let Ok(mut entry) = self.entry(key) {
            entry.suspend();
        }
    }

    pub fn resume(&mut self, key: Key<'d, ID>)
    where
        M::Ingress: Dispatch<W>,
    {
        self.recv().wake_app(key);
    }

    pub fn inspect(&self) -> Inspect<'_, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D> {
        Inspect::new(self)
    }

    pub fn entry(
        &mut self,
        key: Key<'d, ID>,
    ) -> Result<Entry<'_, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>, Missing<'_, 'p, 'w, 'd, ID, W>>
    {
        let prepared = &mut self.prepared;
        if !prepared.slab.contains(key.parts()) {
            return Err(Missing {
                key,
                runtime: &mut prepared.runtime,
                pending: prepared.pending,
            });
        }
        Ok(Entry {
            root: prepared,
            key,
        })
    }

    pub fn get_mut(
        &mut self,
        key: Key<'d, ID>,
    ) -> Option<&mut types::Connection<'w, 'd, ID, W, S>> {
        self.prepared.slab.access().get(key.parts())
    }

    pub fn egress_mut(
        &mut self,
        key: Key<'d, ID>,
    ) -> Option<EgressMut<'_, 'p, 'w, 'd, ID, W, S, B, IOV, D>> {
        let (flights, connection, pending, queue) = self.sender(key)?.entry.into_transmit();
        Some(EgressMut {
            flights,
            connection,
            pending,
            queue: queue.reborrow(),
        })
    }

    pub fn sender(
        &mut self,
        key: Key<'d, ID>,
    ) -> Option<transmit::Sender<'_, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>> {
        self.prepared.pending.handle(key)?;
        Some(transmit::Sender {
            entry: self.entry(key).ok()?,
        })
    }

    pub fn sender_with_recv(
        &mut self,
        key: Key<'d, ID>,
    ) -> Sending<'_, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D> {
        let mut sender = self.sender(key)?;
        let queued = sender.entry.has_queued();
        Some((sender, queued))
    }

    pub fn update_target(
        &mut self,
        target: route::Target<'d, ID>,
        update: impl FnOnce(&mut types::Connection<'w, 'd, ID, W, S>),
    ) -> Option<Key<'d, ID>> {
        let key = Key::from_target(target);
        update(self.prepared.slab.access().get(key.parts())?);
        Some(key)
    }
}

impl<'p, 'w, 'd, ID: Identity + 'w, T, W, S, M, L, R, const IOV: usize, D: demand::Mode>
    Connections<'p, 'w, 'd, ID, T, W, S, M, compose::Either<L, R>, IOV, D>
where
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    M: input::Mode<W>,
    L: data::raw::Payload<'d>,
    R: data::raw::Payload<'d>,
    'd: 'w,
{
    /// Admits one proved right-branch payload and wakes its connection.
    pub fn try_right(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        key: Key<'d, ID>,
        bytes: egress::NonEmpty<R>,
    ) -> Result<(), send::Rejected<R>> {
        if self
            .prepared
            .slab
            .get(key.parts())
            .is_none_or(|slot| slot.send_status().is_closed())
        {
            return Err(send::Rejected::ConnectionUnavailable(bytes));
        }
        let Some(wake) = self.prepared.pending.handle(key) else {
            return Err(send::Rejected::PendingUnavailable(bytes));
        };
        self.prepared.egress.try_right(token, key, bytes)?;
        wake.wake();
        Ok(())
    }
}

impl<
    'p,
    'w,
    'd,
    ID: Identity + 'w,
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    S,
    M,
    B: data::raw::Payload<'d>,
    const IOV: usize,
    D: demand::Mode,
> Pool<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>
where
    M: input::Mode<W>,
    'd: 'w,
{
    pub fn accept_with<F, R>(
        &mut self,
        capacity: conn::Capacity,
        fd: handles::Descriptor<'d>,
        opts: option::Set,
        build: F,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<accept::Outcome<'d, ID, R>, open::Error<'w, W>>
    where
        F: FnOnce(Key<'d, ID>) -> Result<S, R>,
    {
        use o3::depot::arena::slab;

        let keys = *self.storage.keys;
        let flights = &self.storage.prepared.flights;
        let lease = match self.storage.prepared.pending.admit(
            &mut self.storage.prepared.slab,
            keys,
            capacity,
        ) {
            Ok(lease) => lease,
            Err(slab::Vacancy::Full) => {
                fd.close(driver);
                return Ok(accept::Outcome::Unavailable);
            }
            Err(slab::Vacancy::IdentityExhausted) => {
                fd.close(driver);
                return Ok(accept::Outcome::IdentityExhausted);
            }
        };
        let opened = self.storage.prepared.runtime.open(fd, |fd, wire, send| {
            let key = lease.key();
            let state = match build(key) {
                Ok(state) => state,
                Err(reason) => {
                    fd.close(driver);
                    return open::Disposition::retry(accept::Outcome::Rejected(reason), wire, send);
                }
            };
            let (setup, result) = match option::Tuning::try_new(
                &mut driver.reborrow(),
                flights,
                key.target(),
                fd,
                opts,
            ) {
                Ok(option::Tuning::Applied(fd)) => {
                    (setup::Setup::Done(fd), accept::Outcome::Ready(key))
                }
                Ok(option::Tuning::Pending(tune)) => {
                    (setup::Setup::Tuning(tune), accept::Outcome::Pending)
                }
                Err(rejected) => {
                    let (fd, cause) = rejected.into_parts();
                    (setup::Setup::Idle(fd), accept::Outcome::Failed(key, cause))
                }
            };
            lease
                .stage(types::Connection {
                    control: types::Control::new(setup, wire, send, key),
                    state,
                })
                .publish();
            open::Disposition::Commit(result)
        });
        match opened {
            open::Outcome::Unavailable(fd) => {
                fd.close(driver);
                Ok(accept::Outcome::Unavailable)
            }
            open::Outcome::Failed { context: fd, cause } => {
                fd.close(driver);
                Err(cause)
            }
            open::Outcome::Opened(result) => Ok(result),
        }
    }

    pub fn tuned(&mut self, done: tuning::Completion<'d>) -> accept::Completion<'d, ID> {
        let token = done.token();
        let key = match self.storage.keys.parse(token) {
            Ok(key) => key,
            Err(_) => return accept::Completion::Stale(done),
        };
        let Some(mut entry) = self.storage.prepared.slab.access().occupied(key.parts()) else {
            return accept::Completion::Stale(done);
        };
        let step = entry.get_mut().control.engine.tuned(done);
        let key = key.connection();
        match step {
            setup::Completion::Stale(done) => accept::Completion::Stale(done),
            setup::Completion::Failed(error) => accept::Completion::Failed(key, error),
            setup::Completion::Cancelled => accept::Completion::Cancelled(key),
            setup::Completion::Ready => accept::Completion::Ready(key),
        }
    }
}
