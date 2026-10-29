use std::{marker, mem, ops};

use dope_core::{
    driver,
    driver::{
        result, retained,
        route::{self, Identity, KeyTag, table},
    },
    io::{
        event::net::{connect, socket::Completion},
        fd::outbound,
        socket,
        socket::{Open, option, pending},
    },
};
use o3::{
    alloc::{self, array},
    buffer::storage,
    depot::{self, capacity, index::Initialized},
};
use table::conn;

use crate::{
    link::{
        duplex,
        egress::{data, reserve::demand},
        event,
        pool::{self, change::opening, pending::Vacancy, recv::input},
        setup,
        slot::{transmit, types},
    },
    wire::{
        self,
        open::{self, Runtime as _},
        terminal,
    },
};

pub struct Outbound<
    'p,
    'w,
    'd,
    ID: Identity + 'w,
    T: crate::Transport,
    W: wire::Spec,
    S,
    M: input::Mode<W>,
    B = storage::Shared,
    const IOV: usize = 32,
    D: demand::Mode = demand::Fallible,
> where
    'd: 'w,
{
    pub(super) storage: pool::Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
    pub(super) outbound: outbound::Reserved<'d, ID>,
    pub(super) addresses: AddressTable<'d, ID, T::Prepared>,
}

impl<'p, 'w, 'd, ID: Identity + 'w, T, W, S, M, B, const IOV: usize, D> ops::Deref
    for Outbound<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>
where
    'd: 'w,
    T: crate::Transport,
    W: wire::Spec,
    M: input::Mode<W>,
    D: demand::Mode,
{
    type Target = pool::Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>;

    fn deref(&self) -> &Self::Target {
        &self.storage
    }
}

impl<'p, 'w, 'd: 'w, ID: Identity + 'w, T, W, S, M, B, const IOV: usize, D> ops::DerefMut
    for Outbound<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>
where
    T: crate::Transport,
    W: wire::Spec,
    M: input::Mode<W>,
    D: demand::Mode,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.storage
    }
}

impl<'p, 'w, 'd, ID: Identity + 'w, T, W, S, M, B, const IOV: usize, D>
    Outbound<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>
where
    'd: 'w,
    T: crate::Transport,
    W: wire::Spec,
    M: input::Mode<W>,
    D: demand::Mode,
{
    pub fn fds(&self) -> &outbound::Reserved<'d, ID> {
        &self.outbound
    }

    /// Returns the retained target and selected state of an established connection.
    pub fn target<X>(
        &self,
        key: pool::Key<'d, ID>,
        peek: impl FnOnce(&types::Connection<'w, 'd, ID, W, S>) -> X,
    ) -> Option<(T::Prepared, X)> {
        let view = self.storage.inspect();
        let slot = view.get(key)?;
        slot.engine.establish.connected()?;
        Some((*self.addresses.prepared(slot), peek(slot)))
    }

    pub fn send_slot(
        &mut self,
        key: pool::Key<'d, ID>,
    ) -> Option<transmit::Sender<'_, 'p, 'w, 'd, ID, W, S, B, IOV, D>>
    where
        B: data::raw::Payload<'d>,
    {
        let egress = self.storage.sender(key)?;
        if egress.connection.engine.operations.lifecycle.is_closing()
            || egress.connection.engine.operations.sending.is_inflight()
        {
            return None;
        }
        Some(egress)
    }

    pub fn open<P, R>(
        &mut self,
        capacity: conn::Capacity,
        socket: socket::StreamSpec,
        input: P,
        make: impl FnOnce(&P) -> (S, T::Prepared),
        complete: impl FnOnce(&types::Connection<'w, 'd, ID, W, S>) -> R,
        driver: &mut driver::Context<'_, 'd>,
    ) -> opening::Attempt<'d, ID, P, R, open::Error<'w, W>> {
        use pool::pending::{Admission, Grant};

        let keys = *self.storage.keys;
        let grant = match self.storage.prepared.pending.admit(
            &mut self.storage.prepared.slab,
            keys,
            capacity,
        ) {
            Admission::Ready(grant) => grant,
            Admission::Busy | Admission::Full => {
                return opening::Attempt::Deferred {
                    cause: opening::Deferred::Capacity,
                    input,
                };
            }
            Admission::Exhausted => {
                return opening::Attempt::Failed {
                    error: opening::Failure::IdentityExhausted,
                };
            }
        };
        let lease = Grant::reserve(grant);
        let key = lease.key();
        let Some(fd) = self.outbound.bind(lease.key().target()) else {
            return opening::Attempt::Deferred {
                cause: opening::Deferred::Capacity,
                input,
            };
        };
        let address = self.addresses.vacancy(&lease);
        let opened = self.storage.prepared.runtime.open((), |(), wire, send| {
            let open = match Open::try_new(driver, &self.storage.prepared.flights, fd, socket) {
                Ok(open) => open.commit(),
                Err(error) => {
                    let outcome = match error.cause() {
                        result::SubmitCause::Full => opening::Attempt::Deferred {
                            cause: opening::Deferred::Submission(error),
                            input: (),
                        },
                        result::SubmitCause::IdentityExhausted => opening::Attempt::Failed {
                            error: opening::Failure::Submission(error),
                        },
                    };
                    return open::Disposition::retry(outcome, wire, send);
                }
            };
            let (state, target) = make(&input);
            let stored = address.commit(target);
            let engine = duplex::Duplex::targeted(open, stored);
            engine.ready_handle().set_target(key.target().dispatch());
            let output = lease.commit_with(|key| {
                let slot = types::Connection::<ID, W, S> {
                    control: types::Control::new(engine, terminal::State::open()),
                    wire,
                    send,
                    state,
                    key,
                };
                let output = complete(&slot);
                (slot, output)
            });
            open::Disposition::Commit(opening::Attempt::Submitted { key, output })
        });
        match opened {
            open::Outcome::Unavailable(()) => opening::Attempt::Deferred {
                cause: opening::Deferred::WireBackpressure,
                input,
            },
            open::Outcome::Failed { context: (), cause } => opening::Attempt::Failed {
                error: opening::Failure::Wire(cause),
            },
            open::Outcome::Opened(opening::Attempt::Deferred { cause, input: () }) => {
                opening::Attempt::Deferred { cause, input }
            }
            open::Outcome::Opened(opening::Attempt::Submitted { key, output }) => {
                opening::Attempt::Submitted { key, output }
            }
            open::Outcome::Opened(opening::Attempt::Failed { error }) => {
                opening::Attempt::Failed { error }
            }
        }
    }

    pub fn finish_open<X>(
        &mut self,
        done: Completion<'d>,
        driver: &mut retained::Context<'_, '_, 'd>,
        make: impl for<'slot> FnOnce(&'slot types::Connection<'w, 'd, ID, W, S>) -> (X, option::Set),
    ) -> event::Socket<'d, ID, X> {
        use dope_core::io::event::net::socket::{Failure, Outcome};

        let (target, done) = done.into_parts();
        let conns = &mut self.storage;
        let addrs = &self.addresses;
        let key = match conns.keys.parse(target) {
            Ok(key) => key,
            Err(_) => return event::Socket::Stale,
        };
        let prepared = &mut conns.prepared;
        let slots = &prepared.flights;
        let slab = &mut prepared.slab;
        let Some(mut entry) = slab.access().occupied(key.parts()) else {
            return event::Socket::Stale;
        };
        let done = match done {
            Outcome::Created(made) => Ok(made),
            Outcome::Failed(Failure::Create(error)) => Err(error),
            Outcome::Failed(Failure::Retired) => return event::Socket::Stale,
        };
        let (owner, opts) = make(entry.get());
        let cause = match done {
            Err(error) => event::ConnectError::Create(error),
            Ok(made) => {
                let addr = addrs.prepared(entry.get());
                match entry.get_mut().control.engine.created(
                    made,
                    opts,
                    addr,
                    key.target(),
                    slots,
                    driver,
                ) {
                    setup::Submission::Pending => return event::Socket::Pending,
                    setup::Submission::Failed(cause) => match cause {
                        pending::Cause::Submit(error) => event::ConnectError::Admission(error),
                        pending::Cause::Option(error) => event::ConnectError::Option(error),
                    },
                }
            }
        };
        event::Socket::Failed {
            key: key.connection(),
            attempt: owner,
            cause,
        }
    }

    fn complete<X, P>(
        &mut self,
        done: connect::Completion<'d>,
        driver: &mut retained::Context<'_, '_, 'd>,
        peek: impl for<'slot> FnOnce(&'slot types::Connection<'w, 'd, ID, W, S>) -> X,
        peer: impl FnOnce(&AddressTable<'d, ID, T::Prepared>, &types::Connection<'w, 'd, ID, W, S>) -> P,
    ) -> Result<event::Connect<'d, ID, X, P>, connect::Completion<'d>> {
        let key = match self.storage.keys.parse(done.token()) {
            Ok(key) => key,
            Err(_) => return Err(done),
        };
        let prepared = &mut self.storage.prepared;
        let slots = &prepared.flights;
        let slab = &mut prepared.slab;
        let Ok((mut entry, mut recv)) = slab.try_entry(key.parts()) else {
            return Err(done);
        };
        let close = entry.get().engine.operations.lifecycle.is_closing();
        let progress = entry.get_mut().control.engine.complete(done);
        let key = key.connection();
        Ok(match progress {
            setup::Completion::Stale(done) => return Err(done),
            setup::Completion::Failed(error) => event::Connect::Failed {
                key,
                attempt: peek(entry.get()),
                cause: event::ConnectError::Connect(error),
            },
            setup::Completion::Cancelled => event::Connect::Cancelled { key },
            setup::Completion::Ready => {
                if close {
                    return Ok(event::Connect::Cancelled { key });
                }
                let slot = entry.get_mut();
                let attempt = peek(&*slot);
                if let Err(failure) = recv.rearm.submitted(slot.submit_recv(slots, driver)) {
                    return Ok(event::Connect::Failed {
                        key: failure.key,
                        attempt,
                        cause: event::ConnectError::Admission(failure.cause),
                    });
                }
                event::Connect::Connected {
                    key,
                    attempt,
                    peer: peer(&self.addresses, slot),
                }
            }
        })
    }

    pub fn try_finish<X>(
        &mut self,
        done: connect::Completion<'d>,
        driver: &mut retained::Context<'_, '_, 'd>,
        peek: impl for<'slot> FnOnce(&'slot types::Connection<'w, 'd, ID, W, S>) -> X,
    ) -> Result<event::Connect<'d, ID, X, socket::Addr>, connect::Completion<'d>> {
        self.complete(done, driver, peek, |addrs, slot| {
            socket::Addr::copy_from(addrs.prepared(slot))
        })
    }

    pub fn try_attach<X>(
        &mut self,
        done: connect::Completion<'d>,
        driver: &mut retained::Context<'_, '_, 'd>,
        peek: impl for<'slot> FnOnce(&'slot types::Connection<'w, 'd, ID, W, S>) -> X,
    ) -> Result<event::Connect<'d, ID, X, ()>, connect::Completion<'d>> {
        self.complete(done, driver, peek, |_, _| ())
    }
}

pub(super) struct AddressTable<'d, ID: Identity, A: socket::Prepared> {
    pub(super) slots: array::Block<mem::MaybeUninit<A>, capacity::Count>,
    route: marker::PhantomData<route::Target<'d, ID>>,
}

/// One address lane exclusively borrowed for an exact connection vacancy.
struct AddressGrant<'a, 'd, ID: Identity, A: socket::Prepared> {
    slot: &'a mut mem::MaybeUninit<A>,
    route: marker::PhantomData<route::Target<'d, ID>>,
}

pub(in crate::link) struct Stored<'d, ID: Identity> {
    route: marker::PhantomData<route::Target<'d, ID>>,
}

impl<'d, ID: Identity, A: socket::Prepared> AddressTable<'d, ID, A> {
    pub(in crate::link::pool) fn try_new(capacity: table::Capacity) -> Result<Self, alloc::Error> {
        Ok(Self {
            slots: array::Block::try_new(capacity.count(), ())?,
            route: marker::PhantomData,
        })
    }

    fn prepared<'w, W: wire::Spec + 'w, S>(&self, conn: &types::Connection<'w, 'd, ID, W, S>) -> &A
    where
        'd: 'w,
        ID: 'w,
    {
        // SAFETY: these callers retain the live connection from this Outbound's
        // uniquely leased route. Its matching address was committed before publication,
        // and this address-table loan prevents that lane from being recycled.
        unsafe {
            depot::raw::Proof::<_, Initialized<Stored<'d, ID>>>::new((
                self,
                conn.key.index().into_usize(),
            ))
        }
        .into()
    }

    /// Retains the paired physical lane admitted by this exact connection vacancy.
    fn vacancy<'a, U>(
        &'a mut self,
        vacancy: &Vacancy<'_, 'd, ID, U>,
    ) -> AddressGrant<'a, 'd, ID, A> {
        let route = self.route;
        // SAFETY: construction pairs immutable extents; the exact connection
        // vacancy admits this address cell while this loan prevents recycling.
        let slot = unsafe {
            depot::raw::Proof::<_, Initialized<()>>::new((self, vacancy.key().index().into_usize()))
        }
        .into();
        AddressGrant { slot, route }
    }
}

impl<'a, 'd, ID: Identity, A: socket::Prepared> AddressGrant<'a, 'd, ID, A> {
    /// Publishes one prepared address into the granted lane.
    fn commit(self, addr: A) -> Stored<'d, ID> {
        self.slot.write(addr);
        Stored { route: self.route }
    }
}

const _: () = assert!(mem::size_of::<Stored<'static, KeyTag<0>>>() == 0);
