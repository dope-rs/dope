use std::{cell::Cell, rc::Rc};

use dope_core::{driver::route::Identity, io::recv};
use dope_net::wire::{
    self, domain, failure, open, receive,
    receive::{batch, eof, mode, mode::access, reserve},
    send, terminal,
};
use o3::{base::marker::ThreadBound, buffer::storage::inline};

#[derive(Clone, Copy)]
pub struct Failing;

impl failure::Errors<failure::Open> for Failing {
    type Error<'d> = usize;
}

impl failure::Errors<failure::Terminal> for Failing {
    type Error<'d> = usize;
}

impl wire::Spec for Failing {
    type Connection<'d, ID: Identity + 'd> = Self;
    type Storage<ID: Identity> = ();
    type Binding<'d, ID: Identity + 'd> = ();
    type Runtime<'w, 'd, ID: Identity + 'w>
        = Self
    where
        Self: 'w,
        'd: 'w;
    type SendStorage<'d> = ();
    type OpenFailure = failure::Possible;
    type TerminalFailure = failure::Possible;
    type Bootstrap = Self;
    type Receive = receive::Transparent;
    type Send = send::Direct;
}

impl<'w, 'd: 'w, ID: Identity + 'w> open::Runtime<'w, 'd, Failing, ID> for Failing {
    fn open<C, T>(
        &mut self,
        context: C,
        consume: impl FnOnce(C, Failing, ()) -> open::Disposition<T, Failing, ()>,
    ) -> open::Outcome<C, T, open::Error<'w, Failing>> {
        let output = match consume(context, *self, ()) {
            open::Disposition::Commit(output) => output,
            open::Disposition::Retry { output, wire, send } => {
                *self = wire;
                let () = send;
                output
            }
        };
        open::Outcome::Opened(output)
    }
}

#[derive(Default)]
pub struct Buffer<const CAP: usize>(pub inline::Bytes<CAP, u16>, pub ThreadBound);

impl<const CAP: usize> send::Store for Buffer<CAP> {
    fn as_slice(&self) -> &[u8] {
        self.0.as_ref()
    }

    fn release(self) -> send::Availability {
        send::Availability::Unchanged
    }
}

/// The first byte models authenticated final plaintext from a transforming wire.
pub struct Eof {
    input: Rc<Cell<Input>>,
    thread: ThreadBound,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Input {
    Empty,
    Final,
    Unexpected,
}

impl Eof {
    pub fn new(input: Rc<Cell<Input>>) -> Self {
        Self {
            input,
            thread: ThreadBound::NEW,
        }
    }

    fn observe(&self, bytes: &[u8]) {
        if self.input.get() != Input::Unexpected {
            self.input.set(if bytes == b"F" {
                Input::Final
            } else {
                Input::Unexpected
            });
        }
    }
}

impl wire::Spec for Eof {
    type Connection<'d, ID: Identity + 'd> = Self;
    type Storage<ID: Identity> = ();
    type Binding<'d, ID: Identity + 'd> = Self;
    type Runtime<'w, 'd, ID: Identity + 'w>
        = Self
    where
        'd: 'w;
    type SendStorage<'d> = ();
    type OpenFailure = failure::Never;
    type TerminalFailure = failure::Possible;
    type Bootstrap = Self;
    type Receive = Self;
    type Send = send::Direct;
}

impl domain::Init<Eof> for Eof {
    fn runtime<'w, 'd, ID: Identity + 'w>(_: domain::Limits, binding: Self) -> Self
    where
        'd: 'w,
    {
        binding
    }
}

impl<'w, 'd: 'w, ID: Identity + 'w> open::Runtime<'w, 'd, Eof, ID> for Eof {
    fn open<C, T>(
        &mut self,
        context: C,
        consume: impl FnOnce(C, Self, ()) -> open::Disposition<T, Self, ()>,
    ) -> open::Outcome<C, T, open::Error<'w, Eof>> {
        let wire = Self {
            input: Rc::clone(&self.input),
            thread: self.thread,
        };
        open::Outcome::Opened(consume(context, wire, ()).into_output(|_, ()| {}))
    }
}

impl receive::Policy<Eof> for Eof {
    type Batch<'a> = batch::One<&'a [u8]>;
    type Cursor<'w, 'd>
        = recv::Shared<'d>
    where
        'd: 'w;
    type Access = access::Direct;
    type Eof = eof::State;
    type Reserve = reserve::Borrowed;
    type Credit = mode::Enabled;
    type Discard = mode::Disabled;

    fn process<'id, 'a, 'input, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Transaction<'a, 'w, ID, Eof>,
        bytes: &'input mut [u8],
        capacity: batch::Grant<'id, 'd, Eof>,
        end: terminal::Both<'_, terminal::Error<'w, Eof>>,
    ) -> terminal::Processed<batch::Produced<'id, Self::Batch<'input>>>
    where
        'd: 'w,
        'w: 'a + 'input,
    {
        txn.into_wire().observe(bytes);
        end.eof(capacity.one(&*bytes))
    }

    fn read_lease<'a, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Transaction<'a, 'w, ID, Eof>,
        bytes: recv::Lease<'d>,
        end: terminal::Both<'_, terminal::Error<'w, Eof>>,
    ) -> terminal::Processed<Option<Self::Cursor<'w, 'd>>>
    where
        'd: 'w,
        'w: 'a,
    {
        txn.into_wire().observe(bytes.as_slice());
        end.eof(Some(bytes.into_shared()))
    }
}

impl failure::Errors<failure::Terminal> for Eof {
    type Error<'d> = usize;
}

impl eof::Eof<Eof> for Eof {
    fn eof<'w, ID: Identity + 'w>(
        _: &mut Eof,
        end: terminal::Send<'_, terminal::Error<'w, Eof>>,
    ) -> terminal::Processed<()> {
        end.terminal(
            (),
            terminal::Terminal::Failed(terminal::Error::from_cause(7)),
        )
    }
}

pub mod contract {
    use std::{convert::Infallible, time::Duration};

    use dope_core::{
        driver::{
            life::{self, recovery},
            retained,
            route::{KeyTag, table::conn},
            settings::{Config, FileSlots},
        },
        io::{
            Reclaimed,
            event::{self, batch::Flow, net::accept::Failure},
            socket::option,
            transfer,
        },
    };
    use dope_net::{
        link::{
            egress::{self, reserve::demand},
            event::SendOutcome,
            pool::{
                change::{self, accept},
                recv::input,
            },
            slot::transmit::{self, outcome::Retention},
        },
        tcp::Tcp,
        wire::domain::{Local, Resident, Retained},
    };
    use dope_test::{
        alloc::Tracker,
        scope::{self, Driver},
    };
    use o3::buffer::write::Sink as _;

    use super::{Buffer, Identity, domain, failure, mode, open, receive, send, terminal, wire};
    use crate::common::discard::{Error, Peer, next};

    #[derive(Clone, Copy)]
    pub enum Case {
        Buffered,
        Vectored,
    }

    impl wire::Spec for Case {
        type Connection<'d, ID: Identity + 'd> = Self;
        type Storage<ID: Identity> = ();
        type Binding<'d, ID: Identity + 'd> = Self;
        type Runtime<'w, 'd, ID: Identity + 'w>
            = Self
        where
            'd: 'w;
        type SendStorage<'d> = Buffer<8>;
        type OpenFailure = failure::Never;
        type TerminalFailure = failure::Never;
        type Bootstrap = Self;
        type Receive = receive::Transparent<mode::Disabled>;
        type Send = Self;
    }

    impl domain::Init<Case> for Case {
        fn runtime<'w, 'd, ID: Identity + 'w>(_: domain::Limits, binding: Self) -> Self
        where
            'd: 'w,
        {
            binding
        }
    }

    impl<'w, 'd: 'w, ID: Identity + 'w> open::Runtime<'w, 'd, Case, ID> for Case {
        fn open<C, T>(
            &mut self,
            context: C,
            consume: impl FnOnce(C, Self, Buffer<8>) -> open::Disposition<T, Self, Buffer<8>>,
        ) -> open::Outcome<C, T, open::Error<'w, Case>> {
            let mut store = Buffer::default();
            if matches!(self, Self::Buffered) {
                store
                    .0
                    .write_slice(b"retained")
                    .expect("fixed send storage");
            }
            let output = match consume(context, *self, store) {
                open::Disposition::Commit(output) | open::Disposition::Retry { output, .. } => {
                    output
                }
            };
            open::Outcome::Opened(output)
        }
    }

    impl send::Consume for Buffer<8> {
        fn consume_max(&mut self, max: transfer::Len) {
            let len = self.0.len();
            let used = max.into_raw().min(len);
            self.0.as_mut().copy_within(used.., 0);
            self.0.truncate(len - used);
        }
    }

    impl send::Policy<Case> for Case {
        type Reclaim = send::reclaim::OnSubmit;

        fn prepare<'a, 'd, ID: Identity + 'd>(
            context: send::Context<'a, 'd, ID, Case, send::Storage<'a, Buffer<8>>>,
            _: send::Plain<'a>,
            end: terminal::Send<'_, terminal::Error<'d, Case>>,
        ) -> terminal::Processed<send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>>
        where
            'd: 'a,
        {
            Self::flush(context, end)
        }

        fn prepare_many<'a, 'd, ID: Identity + 'd, const N: usize>(
            context: send::Context<'a, 'd, ID, Case, send::Storage<'a, Buffer<8>>>,
            plain: send::Vectored<'a, N>,
            end: terminal::Send<'_, terminal::Error<'d, Case>>,
        ) -> terminal::Processed<send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>>
        where
            'd: 'a,
        {
            let (case, storage) = context.into_parts();
            let (ready, allocations) = Tracker::usage(|| match case {
                Self::Buffered => storage.consume_none().buffered().close_after(),
                Self::Vectored => {
                    let mut plan = send::progress::Plan::vectored(storage, &plain);
                    {
                        let (store, mut input) = plan.parts();
                        assert!(input.take(transfer::Len::MAX).commit().is_zero());
                        {
                            let mut retry = input.take(transfer::Len::MAX);
                            assert_eq!(retry.take().next().expect("first chunk").bytes(), b"ab");
                        }
                        assert_eq!(input.remaining(), transfer::Len::fixed::<6>());
                        let mut first = input.take(transfer::Len::MAX);
                        store
                            .0
                            .write_slice(first.take().next().expect("same first chunk").bytes())
                            .expect("bounded send storage");
                        assert_eq!(first.commit(), transfer::Len::fixed::<2>());
                    }
                    for limit in [transfer::Len::fixed::<1>(), transfer::Len::MAX] {
                        let (store, mut input) = plan.parts();
                        let mut parts = input.take(limit);
                        for part in parts.take() {
                            store
                                .0
                                .write_slice(part.bytes())
                                .expect("bounded send storage");
                        }
                        parts.commit();
                    }
                    let (store, input) = plan.parts();
                    assert!(input.is_empty());
                    assert_eq!(input.remaining(), transfer::Len::ZERO);
                    assert_eq!(store.0.as_ref(), b"abcdef");
                    plan.finish().buffered().close_after()
                }
            });
            assert_eq!(allocations, (0, 0));
            end.output(ready)
        }

        fn after_send<'a, 'd, ID: Identity + 'd>(
            context: send::Context<
                'a,
                'd,
                ID,
                Case,
                send::Completion<'a, Self::Reclaim, Buffer<8>>,
            >,
            end: terminal::Send<'_, terminal::Error<'d, Case>>,
        ) -> terminal::Processed<send::Transition<'a, Self::Reclaim, impl send::Payload<'a>>>
        where
            'd: 'a,
        {
            end.output(send::Transition::unchanged(
                context.into_state().consume_max().buffered(),
            ))
        }

        fn settle<'a, 'd, ID: Identity + 'd>(
            context: send::Context<
                'a,
                'd,
                ID,
                Case,
                send::Completion<'a, Self::Reclaim, Buffer<8>>,
            >,
            _: Option<terminal::Drain<'_, 'd, Case>>,
        ) -> send::Transition<'a, Self::Reclaim, impl send::Payload<'a>>
        where
            'd: 'a,
        {
            send::Transition::unchanged(context.into_state().consume_max().buffered())
        }

        fn flush<'a, 'd, ID: Identity + 'd>(
            context: send::Context<'a, 'd, ID, Case, send::Storage<'a, Buffer<8>>>,
            end: terminal::Send<'_, terminal::Error<'d, Case>>,
        ) -> terminal::Processed<send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>>
        where
            'd: 'a,
        {
            end.output(context.into_state().buffered())
        }

        fn drain<'a, 'd, ID: Identity + 'd>(
            context: send::Context<'a, 'd, ID, Case, send::Storage<'a, Buffer<8>>>,
            _: Option<terminal::Drain<'_, 'd, Case>>,
        ) -> send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>
        where
            'd: 'a,
        {
            context.into_state().buffered()
        }
    }

    impl Case {
        pub fn run(self) -> (Vec<u8>, bool, bool) {
            type Route = KeyTag<24>;
            let config = Config::default().with_files(FileSlots::fixed::<1, 1>());
            let ((queued, closing, sent, buffered), returned, stream) = Driver::new(config)
                .with_scope(
                    |scope| {
                        let (peer, fd) = Peer::new(scope)?;
                        change::try_prepare::<
                            Route,
                            Tcp,
                            Self,
                            (),
                            input::Borrowed,
                            &[u8],
                            8,
                            _,
                            _,
                            _,
                            _,
                        >(
                            Retained::ZERO,
                            Resident::ZERO,
                            demand::Fallible::prepare(
                                egress::Config::DEFAULT,
                                conn::Capacity::fixed::<1>(),
                            ),
                            Local::new(self),
                            scope,
                            life::Scope::context,
                            |(prepared, need), scope| {
                                let mut pool = {
                                    let mut driver = scope.context();
                                    let flights = driver
                                        .storage
                                        .reborrow()
                                        .try_flights(need)
                                        .map_err(Error::Allocation)?
                                        .slots();
                                    let route = driver
                                        .storage
                                        .reborrow()
                                        .try_reserve::<Route>()
                                        .map_err(Error::Route)?;
                                    // SAFETY: the same pool and all queued sources remain live
                                    // through native quiescence and pool.finish below on every exit.
                                    unsafe { prepared.commit(flights).bind(route) }
                                };
                                let result = (|| {
                                    let key = {
                                        let mut driver = scope.context();
                                        let opened = match pool.accept_with(
                                            conn::Capacity::fixed::<1>(),
                                            fd,
                                            option::Set::EMPTY,
                                            |_| Ok::<_, Infallible>(()),
                                            &mut driver,
                                        ) {
                                            Ok(opened) => opened,
                                            Err(cause) => cause.eliminate(),
                                        };
                                        match opened {
                                            accept::Outcome::Ready(key) => key,
                                            accept::Outcome::Pending => return Err(Error::Pending),
                                            accept::Outcome::Failed(_, cause) => {
                                                return Err(Error::Failed(cause));
                                            }
                                            accept::Outcome::Unavailable => {
                                                return Err(Error::Unavailable);
                                            }
                                            accept::Outcome::IdentityExhausted => {
                                                return Err(Error::Exhausted);
                                            }
                                            accept::Outcome::Rejected(cause) => match cause {},
                                        }
                                    };
                                    {
                                        let mut driver = scope.context();
                                        let queue =
                                            pool.egress_mut(key).ok_or(Error::Connection)?.queue;
                                        let input: [&[u8]; 2] = match self {
                                            Self::Buffered => [b"keep", b""],
                                            Self::Vectored => [b"ab", b"cdef"],
                                        };
                                        for bytes in input {
                                            if !bytes.is_empty()
                                                && !matches!(
                                                    queue.try_enqueue(driver.region_token(), bytes),
                                                    egress::Enqueue::Committed
                                                )
                                            {
                                                return Err(Error::Unavailable);
                                            }
                                        }
                                        let owner =
                                            retained::raw::Owner::from_driver(driver.driver_ref());
                                        let mut driver = retained::Context::new(driver, owner);
                                        match pool
                                            .sender(key)
                                            .ok_or(Error::Sender)?
                                            .submit(&mut driver)
                                        {
                                            transmit::Attempt::Rejected(cause, _) => {
                                                return Err(Error::Rejected(cause));
                                            }
                                            transmit::Attempt::Invalid(cause) => {
                                                return Err(Error::Invalid(cause));
                                            }
                                            _ => {}
                                        }
                                    }
                                    let (queued, closing) = {
                                        let entry =
                                            pool.egress_mut(key).ok_or(Error::Connection)?;
                                        if !entry.connection.send_status().is_inflight() {
                                            return Err(Error::Submitted);
                                        }
                                        (!entry.queue.is_empty(), entry.connection.is_draining())
                                    };
                                    let done = match next(scope)?.into_kind() {
                                        event::Kind::Send(done) => done,
                                        _ => return Err(Error::Submitted),
                                    };
                                    let sent = done.outcome();
                                    let mut buffered = false;
                                    {
                                        let mut driver = scope.context();
                                        let owner =
                                            retained::raw::Owner::from_driver(driver.driver_ref());
                                        let mut driver =
                                            retained::Context::new(driver.reborrow(), owner);
                                        let settled = pool.settle(done).settle_owned(
                                            &mut driver,
                                            |slot, _| {
                                                buffered = matches!(
                                                    slot.send_status().retention(),
                                                    Retention::Held
                                                );
                                            },
                                        );
                                        if !matches!(
                                            settled,
                                            SendOutcome::Queued { .. }
                                                | SendOutcome::CloseSent { .. }
                                        ) {
                                            return Err(Error::Submitted);
                                        }
                                    }
                                    Ok((queued, closing, sent, buffered))
                                })();
                                let (flight, stream) = peer.close(scope);
                                let mut returned = 0;
                                scope.finish(true, |_, finish| {
                                    pool.finish(finish, |bytes, _| returned += bytes.len())
                                });
                                drop(flight);
                                result.map(|observed| (observed, returned, stream))
                            },
                        )
                        .map_err(Error::Resource)?
                    },
                    recovery::Policy::new(
                        |event| {
                            assert!(
                                matches!(event, Reclaimed::Accept {
                        result: Err(Failure::Syscall(error)), flow: Flow::Last, ..
                    } if error.is_canceled()),
                                "unexpected reclamation: {event:?}"
                            )
                        },
                        |failure: recovery::Recovery<'_, '_>| -> Infallible {
                            panic!("driver recovery: {:?}", failure.cause())
                        },
                    ),
                    |result| {
                        result
                            .as_ref()
                            .map_err(scope::Error::cause)
                            .expect("native lifecycle");
                        result.ok().expect("driver output").expect("send fixture")
                    },
                );
            let len = match self {
                Self::Buffered => 8,
                Self::Vectored => 6,
            };
            assert_eq!(
                sent,
                Ok(transfer::Len::try_new(len).expect("bounded output"))
            );
            assert!(!buffered, "completion must release the buffered prefix");
            assert_eq!(returned, if matches!(self, Self::Buffered) { 4 } else { 0 });
            stream
                .read_wait(Some(Duration::from_secs(5)))
                .expect("peer timeout");
            let mut bytes = vec![0; len];
            stream.read_exact(&mut bytes).expect("submitted bytes");
            (bytes, queued, closing)
        }
    }
}
