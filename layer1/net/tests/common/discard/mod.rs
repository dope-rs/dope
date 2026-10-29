use std::{
    convert::Infallible,
    io,
    net::{SocketAddr, TcpStream},
    ops::ControlFlow,
    time::{Duration, Instant},
};

use dope_core::{
    driver::{
        life::{self, recovery},
        result::{self, poll, poll::fault},
        retained::{self, flight},
        route::{Epoch, KeyTag, indices::SlotIndex, kind, lease, table::conn},
        settings::Config,
    },
    io::{
        Event,
        event::{
            self,
            batch::Flow,
            net::{
                accept::{Failure, Outcome},
                recv,
            },
        },
        fd::handles,
        socket::{self, option, pending},
        transfer,
    },
};
use dope_net::{
    Listen,
    link::{
        egress::{self, reserve::demand},
        event::{SendFailure, SendOutcome},
        pool::{
            change::{self, accept},
            recv::input,
        },
        slot::transmit::{self, outcome},
    },
    tcp::{self, Tcp},
    wire::domain::{Local, Resident, Retained},
};
use dope_test::scope::{self, Driver};
use o3::alloc;

use crate::common::segments::Wire;

pub mod eof;

#[derive(Debug)]
pub enum Error {
    Bind(dope_net::BindingError),
    Allocation(alloc::Error),
    Submit(result::SubmitError),
    Rejected(SendFailure<result::SubmitError>),
    Invalid(transmit::Invalid),
    Peer(io::Error),
    Dispatch(fault::Dispatch),
    Wait(poll::Error),
    Deadline {
        duration: Duration,
    },
    Timeout,
    Flight,
    Accept(Failure),
    Resource(dope_net::Resource),
    Route(lease::Error),
    Event {
        expected: kind::Kind,
        actual: kind::Kind,
    },
    Pending,
    Failed(pending::Cause),
    Unavailable,
    Exhausted,
    Sender,
    Connection,
    Submitted,
    ReceiveSubmit(result::SubmitError),
    ReceiveEof,
    ReceiveCancelled,
    ReceiveExhausted,
    ReceiveStarved(recv::Retryable),
    ReceiveFailed(dope_core::Errno),
    ReceiveData(transfer::Len),
}

impl Error {
    fn receive(status: recv::Status) -> Self {
        match status {
            recv::Status::Eof => Self::ReceiveEof,
            recv::Status::Cancelled => Self::ReceiveCancelled,
            recv::Status::Exhausted => Self::ReceiveExhausted,
            recv::Status::Starved(error) => Self::ReceiveStarved(error),
            recv::Status::Failed(error) => Self::ReceiveFailed(error),
        }
    }

    fn event(expected: kind::Kind, event: event::Kind<'_>) -> Self {
        let actual = match event {
            event::Kind::Accept(_) => kind::Kind::Accept,
            event::Kind::Recv(_) | event::Kind::RetiredRecv(_) => kind::Kind::Recv,
            event::Kind::Send(_) => kind::Kind::Send,
            event::Kind::Socket(_) => kind::Kind::Socket,
            event::Kind::Tuning(_) => kind::Kind::Tuning,
            event::Kind::Connect(_) => kind::Kind::Connect,
            event::Kind::Open(_) => kind::Kind::Open,
            event::Kind::Write(_) => kind::Kind::Write,
            event::Kind::Sync(_) => kind::Kind::Sync,
            event::Kind::Shutdown => kind::Kind::Shutdown,
        };
        Self::Event { expected, actual }
    }
}

/// Captured before quiescence, while the second segment still belongs to the pool.
pub struct Observed {
    pub sent: Result<transfer::Len, dope_core::Errno>,
    pub resubmit: transmit::Attempt,
    pub inflight: bool,
    pub buffered: bool,
    pub pending_accept: bool,
}

pub fn next<'d>(scope: &mut life::Scope<'_, 'd>) -> Result<Event<'d>, Error> {
    let duration = Duration::from_secs(5);
    let deadline = Instant::now()
        .checked_add(duration)
        .ok_or(Error::Deadline { duration })?;
    loop {
        let event = scope.with_driver(|mut driver, mut control| {
            let mut turn = control.begin();
            match driver.dispatch(turn.reactor(), |event, _| ControlFlow::Break(event)) {
                poll::Dispatch::Ready {
                    retained: Some(event),
                    ..
                } => return Ok(Some(event)),
                poll::Dispatch::Ready { retained: None, .. } => {}
                poll::Dispatch::Fault(cause) => return Err(Error::Dispatch(cause)),
            }
            let left = deadline
                .checked_duration_since(Instant::now())
                .ok_or(Error::Timeout)?;
            driver
                .wait(turn.reactor(), Some(left))
                .map_err(Error::Wait)?;
            Ok(None)
        })?;
        if let Some(event) = event {
            return Ok(event);
        }
    }
}

pub struct Peer<'d> {
    listener: handles::Descriptor<'d>,
    flight: Option<flight::Flight<'d, kind::Accept>>,
    pub stream: TcpStream,
}

impl<'d> Peer<'d> {
    pub fn new(scope: &mut life::Scope<'_, 'd>) -> Result<(Self, handles::Descriptor<'d>), Error> {
        let mut peer = Self::listen(scope)?;
        let fd = match next(scope)?.into_kind() {
            event::Kind::Accept(done) => {
                let (_, flow, result) = done.into_parts();
                if flow == Flow::Last {
                    drop(peer.flight.take().ok_or(Error::Flight)?);
                }
                match result {
                    Outcome::Accepted(fd) => fd.bind(),
                    Outcome::Failed(cause) => return Err(Error::Accept(cause)),
                }
            }
            other => return Err(Error::event(kind::Kind::Accept, other)),
        };
        Ok((peer, fd))
    }

    fn listen(scope: &mut life::Scope<'_, 'd>) -> Result<Self, Error> {
        let mut driver = scope.context();
        let (listener, address) = Tcp::bind(
            &mut driver,
            &SocketAddr::from(([127, 0, 0, 1], 0)),
            socket::Backlog::fixed::<1>(),
            &tcp::Config::default(),
        )
        .map_err(Error::Bind)?;
        let slots = driver
            .storage
            .reborrow()
            .try_flights(flight::Need::<KeyTag<25>>::new(
                flight::Capacity::fixed::<1>(),
            ))
            .map_err(Error::Allocation)?
            .slots();
        let target = driver
            .driver_ref()
            .targets::<KeyTag<25>>()
            .bind(SlotIndex::ZERO, Epoch::INITIAL);
        let flight = slots
            .accept(
                driver.reborrow(),
                &listener,
                target.operation::<kind::Accept>(),
            )
            .map_err(Error::Submit)?;
        let stream = TcpStream::connect(address).map_err(Error::Peer)?;
        Ok(Self {
            listener,
            flight: Some(flight),
            stream,
        })
    }

    pub fn close(
        self,
        scope: &mut life::Scope<'_, 'd>,
    ) -> (Option<flight::Flight<'d, kind::Accept>>, TcpStream) {
        self.listener.close(&mut scope.context());
        (self.flight, self.stream)
    }
}

impl Observed {
    /// Discards before completion polling, settles the first segment, then tries
    /// another flush. Quiescence drains any erroneous send before pool release.
    pub fn capture<R>(
        config: Config,
        settled: impl for<'d> FnOnce(SendOutcome<'d, KeyTag<24>>),
        policy: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(
            Result<Result<Self, Error>, scope::Error<'owner, Result<Self, Error>>>,
        ) -> R,
    ) -> R {
        type Route = KeyTag<24>;
        Driver::new(config).with_scope(
            |scope| {
                let (peer, fd) = Peer::new(scope)?;
                change::try_prepare::<Route, Tcp, Wire, (), input::Borrowed, &[u8], 32, _, _, _, _>(
                    Retained::ZERO,
                    Resident::ZERO,
                    demand::Fallible::prepare(
                        egress::Config::DEFAULT,
                        conn::Capacity::fixed::<1>(),
                    ),
                    Local::new(()),
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
                            // SAFETY: every post-commit operation returns into result below.
                            // Pool storage stays alive through mandatory native quiescence
                            // and pool.finish before that result or this owner can leave.
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
                                    accept::Outcome::Unavailable => return Err(Error::Unavailable),
                                    accept::Outcome::IdentityExhausted => {
                                        return Err(Error::Exhausted);
                                    }
                                    accept::Outcome::Rejected(cause) => match cause {},
                                }
                            };
                            {
                                let mut driver = scope.context();
                                let owner = retained::raw::Owner::from_driver(driver.driver_ref());
                                let mut driver = retained::Context::new(driver.reborrow(), owner);
                                match pool.sender(key).ok_or(Error::Sender)?.submit(&mut driver) {
                                    transmit::Attempt::Rejected(cause, _) => {
                                        return Err(Error::Rejected(cause));
                                    }
                                    transmit::Attempt::Invalid(cause) => {
                                        return Err(Error::Invalid(cause));
                                    }
                                    _ => {}
                                }
                                let slot = pool.get_mut(key).ok_or(Error::Connection)?;
                                if !slot.send_status().is_inflight() {
                                    return Err(Error::Submitted);
                                }
                                slot.sending().discard();
                            }
                            let done = match next(scope)?.into_kind() {
                                event::Kind::Send(done) => done,
                                other => return Err(Error::event(kind::Kind::Send, other)),
                            };
                            let sent = done.outcome();
                            let resubmit = {
                                let mut driver = scope.context();
                                let owner = retained::raw::Owner::from_driver(driver.driver_ref());
                                let mut driver = retained::Context::new(driver.reborrow(), owner);
                                settled(pool.settle(done).settle_owned(&mut driver, |_, _| {}));
                                pool.sender(key).ok_or(Error::Sender)?.submit(&mut driver)
                            };
                            let status = pool.get_mut(key).ok_or(Error::Connection)?.send_status();
                            Ok(Observed {
                                sent,
                                resubmit,
                                inflight: status.is_inflight(),
                                buffered: matches!(status.retention(), outcome::Retention::Held),
                                pending_accept: peer.flight.is_some(),
                            })
                        })();
                        let flight = peer.close(scope);
                        scope.finish(true, |_, finish| pool.finish(finish, |_, _| {}));
                        drop(flight);
                        result
                    },
                )
                .map_err(Error::Resource)?
            },
            policy,
            finish,
        )
    }
}
