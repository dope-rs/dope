use std::ops;

use dope_core::{
    driver::{
        self, lifecycle,
        ops::Control as _,
        retained,
        route::{self, kind},
    },
    io::{
        self,
        event::{connect, creation},
        socket::{self, option},
    },
};
use o3::buffer::storage;

use crate::{
    link::{
        self,
        egress::data,
        event,
        pool::{self, pending, transition::open},
        setup,
        slot::types,
    },
    wire,
};

mod sealed;

pub(super) use sealed::AddressTable;
pub(in crate::link) use sealed::StoredAddress;

pub struct Outbound<
    'd,
    const ID: u8,
    T: crate::Transport,
    W: wire::Wire,
    S,
    M,
    B = storage::Shared,
    const IOV: usize = 32,
> {
    pub(super) storage: pool::Connections<'d, ID, T, W, S, M, B, IOV>,
    pub(super) outbound: pool::raw::Reservation<'d, ID>,
    pub(super) addresses: AddressTable<'d, ID>,
}

enum ConnectOutcome<'d, const ID: u8, X> {
    Connected {
        attempt: X,
        peer: socket::Addr,
        armed: bool,
        rearm: pool::Key<'d, ID>,
    },
    Failed {
        attempt: X,
        cause: event::ConnectFailure,
    },
}

enum Vacancy {
    In(ops::Range<u32>),
    At(route::SlotIndex),
}

impl<'d, const ID: u8, T: crate::Transport, W: wire::Wire, S, M, B, const IOV: usize> ops::Deref
    for Outbound<'d, ID, T, W, S, M, B, IOV>
{
    type Target = pool::Connections<'d, ID, T, W, S, M, B, IOV>;

    fn deref(&self) -> &Self::Target {
        &self.storage
    }
}

impl<const ID: u8, T: crate::Transport, W: wire::Wire, S, M, B, const IOV: usize> ops::DerefMut
    for Outbound<'_, ID, T, W, S, M, B, IOV>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.storage
    }
}

impl<'d, const ID: u8, T, W, S, M, B, const IOV: usize> Outbound<'d, ID, T, W, S, M, B, IOV>
where
    T: crate::Transport,
    W: wire::Wire,
{
    pub fn send_slot(
        &mut self,
        key: pool::Key<'d, ID>,
    ) -> Option<pending::ScheduledEgress<'_, 'd, ID, W, S, B, IOV>>
    where
        B: data::Payload<'d>,
    {
        let egress = pending::Mut::of(&mut self.storage).egress(key)?;
        if egress.connection.engine.lifecycle.is_closing()
            || egress.connection.engine.sending.is_inflight()
        {
            return None;
        }
        Some(egress)
    }

    pub fn submit_socket<P, R>(
        &mut self,
        range: ops::Range<u32>,
        socket: socket::StreamSpec,
        input: P,
        make_state: impl FnOnce(P) -> (S, Option<socket::Addr>, R),
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<open::Outcome<'d, ID, P, R>, open::Rejected<P, W::OpenError>> {
        self.submit_socket_with(Vacancy::In(range), socket, input, make_state, driver)
    }

    pub fn submit_socket_at<P, R>(
        &mut self,
        lane: route::SlotIndex,
        socket: socket::StreamSpec,
        input: P,
        make_state: impl FnOnce(P) -> (S, Option<socket::Addr>, R),
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<open::Outcome<'d, ID, P, R>, open::Rejected<P, W::OpenError>> {
        self.submit_socket_with(Vacancy::At(lane), socket, input, make_state, driver)
    }

    fn submit_socket_with<P, R>(
        &mut self,
        vacancy: Vacancy,
        socket: socket::StreamSpec,
        input: P,
        make_state: impl FnOnce(P) -> (S, Option<socket::Addr>, R),
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<open::Outcome<'d, ID, P, R>, open::Rejected<P, W::OpenError>> {
        let keys = self.storage.keys;
        let reservation = match vacancy {
            Vacancy::In(range) => {
                let Some(reservation) = self.storage.prepared.slab.vacant_entry_in(range) else {
                    return Ok(open::Outcome::Deferred {
                        cause: open::Deferred::Capacity,
                        input,
                    });
                };
                reservation
            }
            Vacancy::At(lane) => {
                let Some(reservation) = self.storage.prepared.slab.vacant_entry_at(lane.raw())
                else {
                    return Ok(open::Outcome::Deferred {
                        cause: open::Deferred::Capacity,
                        input,
                    });
                };
                reservation
            }
        };
        let reservation = self.storage.prepared.scheduling.reserve(keys, reservation);
        let key = reservation.key();
        let Some(lane) = self.storage.prepared.egress.lane(key.index()) else {
            return Ok(open::Outcome::Deferred {
                cause: open::Deferred::Capacity,
                input,
            });
        };
        let Some(fd) = self.outbound.descriptor(&reservation) else {
            return Ok(open::Outcome::Deferred {
                cause: open::Deferred::Capacity,
                input,
            });
        };
        let open = match W::prepare_open(&mut self.storage.prepared.runtime) {
            Ok(Some(open)) => open,
            Ok(None) => {
                drop(fd);
                return Ok(open::Outcome::Deferred {
                    cause: open::Deferred::WireBackpressure,
                    input,
                });
            }
            Err(error) => {
                drop(fd);
                return Err(open::Rejected::new(input, open::Failure::Wire(error)));
            }
        };
        let creating = match driver.submit_socket(&self.storage.prepared.flights, fd, socket) {
            Ok(creating) => creating,
            Err(_) => {
                drop(open);
                return Ok(open::Outcome::Deferred {
                    cause: open::Deferred::SubmissionBackpressure,
                    input,
                });
            }
        };
        let (state, target, output) = make_state(input);
        let engine = match target {
            Some(target) => {
                let stored = self.addresses.store(&reservation, target);
                link::Engine::outbound_targeted(creating, stored)
            }
            None => link::Engine::outbound(creating),
        };
        let (wire, send) = wire::OpenReservation::commit(open);
        reservation.commit_with(|key| {
            (
                types::Stored::new(
                    types::Connection::<ID, W, S>::new(engine, wire, send, key, state),
                    lane,
                ),
                (),
            )
        });
        self.storage.refresh_wake(key);
        Ok(open::Outcome::Submitted { key, output })
    }

    pub fn complete_socket<X>(
        &mut self,
        completion: creation::Completion<'d>,
        driver: &mut retained::Context<'_, '_, 'd>,
        prepare: impl for<'slot> FnOnce(
            &'slot types::Connection<'d, ID, W, S>,
        ) -> (X, Option<option::StreamOptions>),
    ) -> event::Socket<'d, ID, X> {
        let (target, socket_event) = completion.into_parts();
        let Self {
            storage, addresses, ..
        } = self;
        let Some(key) = storage.keys.parse(target) else {
            return event::Socket::Stale;
        };
        if target != route::Token::from(key).with_kind(kind::SOCKET) {
            return event::Socket::Stale;
        }
        let pool::Prepared { flights, slab, .. } = &mut storage.prepared;
        let Some(mut entry) = slab.occupied_entry_parts(key.parts()) else {
            return event::Socket::Stale;
        };
        let slot = entry.get_mut();
        let (attempt, options) = prepare(&*slot);
        let cause = match socket_event {
            io::SocketEvent::Failed(error) => event::ConnectFailure::Socket(error),
            io::SocketEvent::Created(created) => {
                let engine = &mut slot.engine;
                match engine.establish.created(created, options, |fd, options| {
                    let addr = addresses.get(key);
                    link::Connect::new(fd, addr, key.target()).submit(flights, driver, options)
                }) {
                    setup::Submission::Missing => event::ConnectFailure::NoTarget,
                    setup::Submission::Pending => return event::Socket::Pending,
                    setup::Submission::Failed(error) => event::ConnectFailure::Admission(error),
                }
            }
        };
        event::Socket::Failed {
            key,
            attempt,
            cause,
        }
    }

    pub fn complete_connect<X>(
        &mut self,
        completion: connect::Completion,
        driver: &mut retained::Context<'_, '_, 'd>,
        peek: impl for<'slot> FnOnce(&'slot types::Connection<'d, ID, W, S>) -> X,
    ) -> event::Connect<'d, ID, X> {
        use crate::link::event::Connect;

        let target = completion.token();
        let Some(key) = self.storage.keys.parse(target) else {
            return Connect::Stale;
        };
        if target != route::Token::from(key).with_kind(kind::CONNECT) {
            return Connect::Stale;
        }
        let addresses = &self.addresses;
        let pool::Prepared {
            flights,
            slab,
            scheduling,
            ..
        } = &mut self.storage.prepared;
        let Some(mut entry) = slab.occupied_entry_parts(key.parts()) else {
            return Connect::Stale;
        };
        let outcome = {
            let slot = entry.get_mut();
            let established = slot
                .engine
                .establish
                .complete(completion, || *addresses.get(key));
            match established {
                setup::Completion::Idle | setup::Completion::Done => {
                    return Connect::Stale;
                }
                setup::Completion::Failed(error) => ConnectOutcome::Failed {
                    attempt: peek(&*slot),
                    cause: event::ConnectFailure::Connect(error),
                },
                setup::Completion::Connected(peer) => ConnectOutcome::Connected {
                    attempt: peek(&*slot),
                    peer,
                    armed: pool::Connections::<ID, T, W, S, M, B, IOV>::submit_recv(
                        slot,
                        flights,
                        key.target(),
                        driver,
                    ),
                    rearm: slot.key(),
                },
            }
        };
        match outcome {
            ConnectOutcome::Failed { attempt, cause } => Connect::Failed {
                key,
                attempt,
                cause,
            },
            ConnectOutcome::Connected {
                attempt,
                peer,
                armed,
                rearm,
            } => {
                if !armed {
                    scheduling.rearm.queue(rearm);
                }
                Connect::Connected { key, attempt, peer }
            }
        }
    }

    pub fn has_outbound_targets(&self) -> bool {
        self.storage.inspection().has_outbound_targets_unchecked()
    }

    #[doc(hidden)]
    pub fn finish(&mut self, finish: &mut lifecycle::Finalize<'_, 'd>) {
        assert!(
            self.storage.prepared.slab.is_empty(),
            "retained connection owner reached finish before quiescence"
        );
        finish.retire_route(&self.storage.route);
    }
}
