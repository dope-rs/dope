use std::{mem, net, pin};

use dope_core::{
    Backend,
    dope_sys::io::socket::accept::Source,
    driver::{
        self,
        life::finish,
        result,
        retained::{
            self,
            accept::{Retire, Retired},
            arm, flight,
        },
        route::{self, Identity, indices, kind, table},
        schedule::{self, clock, maint},
    },
    io::{
        event::{batch, net::accept},
        fd::handles,
        socket::{self, option},
    },
};
use dope_net::link::pool::{self};
use dope_runtime::random;
use o3::{
    alloc,
    base::{cell::region, time::Positive},
    depot::capacity,
    mem::quota::{self, Acquire},
};
pub(in crate::network::listener) use peers::Admission;
use peers::Quota;
use table::conn::Capacity;

use crate::{
    app::{creation, env::quota::PerIp},
    network::listener::AcceptError,
};

mod mode;
mod peers;
pub(in crate::network::listener) mod phase;

pub(in crate::network::listener) enum Incoming<'d> {
    Accepted(handles::Descriptor<'d>, Option<net::IpAddr>),
    Invalid(handles::Descriptor<'d>, socket::Error),
    Failed {
        target: route::Erased<'d, kind::Accept>,
        cause: accept::Failure,
        flow: batch::Flow,
        defer: bool,
    },
    Rejected,
}

enum Rejection<E> {
    Peer(peers::Full),
    Application(E),
}

pub(in crate::network::listener) enum Prepare {
    Capacity(capacity::Error),
    Allocation(alloc::Error),
}

impl Prepare {
    pub(in crate::network::listener) fn into_error(self) -> creation::Error {
        match self {
            Self::Capacity(cause) => {
                creation::Error::Config(crate::ConfigError::ListenerPeerCapacity(cause))
            }
            Self::Allocation(cause) => creation::Error::Allocation(cause),
        }
    }
}

const MAX_ONESHOT_LANES: usize = {
    let native = <Backend as Source>::MAX_IN_FLIGHT.get();
    if native < schedule::LIMIT {
        native
    } else {
        schedule::LIMIT
    }
};
// Bound persistent exhaustion to 100 retry batches per second without a timer slot.
const RETRY_DELAY: Positive = Positive::from_millis::<10>();
const _: () = {
    assert!(MAX_ONESHOT_LANES <= u16::MAX as usize);
    assert!(mem::size_of::<Prepare>() == 2 * mem::size_of::<usize>());
    assert!(mem::align_of::<Prepare>() == mem::align_of::<usize>());
    assert!(!mem::needs_drop::<Prepare>());
};
#[pin_project::pin_project(!Unpin)]
pub(in crate::network::listener) struct Accept<
    'd,
    ID: Identity,
    F = flight::Slots<'d, <ID as route::Materialize<kind::Accept>>::With>,
> {
    #[pin]
    fd: handles::Descriptor<'d>,
    flights: F,
    mode: mode::Mode<'d, ID>,
    capacity: Capacity,
    options: option::Set,
    quota: Quota<'d, ID>,
    failure: Option<AcceptError>,
    retry: Option<clock::Deadline<'d>>,
}

pub(in crate::network::listener) struct Prepared<'d, ID: Identity> {
    capacity: table::Capacity,
    quota: Quota<'d, ID>,
}

impl<'d, ID: Identity> Prepared<'d, ID> {
    pub(in crate::network::listener) fn try_new(
        capacity: table::Capacity,
        policy: PerIp,
        hash: random::HashState<'d>,
    ) -> Result<Self, Prepare> {
        Ok(Self {
            capacity,
            quota: Quota::try_new(capacity, policy, hash)?,
        })
    }

    pub(in crate::network::listener) fn bind(
        self,
        fd: handles::Descriptor<'d>,
        capacity: Capacity,
        opts: option::Set,
    ) -> Result<
        Accept<'d, ID, flight::Need<<ID as route::Materialize<kind::Accept>>::With>>,
        alloc::Error,
    > {
        let Prepared {
            capacity: total,
            quota,
        } = self;
        let edge = indices::Edge::from(capacity);
        let routes = fd
            .driver()
            .targets::<<ID as route::Materialize<kind::Accept>>::With>();
        let (flights, mode) = if quota.is_limited() {
            let cap = total.count().get().min(MAX_ONESHOT_LANES) as u16;
            let flights = flight::Need::new(flight::Capacity::capped::<MAX_ONESHOT_LANES>(total));
            (flights, mode::Mode::try_oneshot(routes, edge, cap)?)
        } else {
            (
                flight::Need::new(flight::Capacity::fixed::<1>()),
                mode::Mode::multishot(routes, edge),
            )
        };
        Ok(Accept {
            fd,
            flights,
            mode,
            capacity,
            options: opts,
            quota,
            failure: None,
            retry: None,
        })
    }
}

impl<'d, ID: Identity>
    Accept<'d, ID, flight::Need<<ID as route::Materialize<kind::Accept>>::With>>
{
    pub(in crate::network::listener) fn into_parts(
        self,
    ) -> (
        Accept<'d, ID, ()>,
        flight::Need<<ID as route::Materialize<kind::Accept>>::With>,
    ) {
        let Self {
            fd,
            flights,
            mode,
            capacity,
            options,
            quota,
            failure,
            retry,
        } = self;
        (
            Accept {
                fd,
                flights: (),
                mode,
                capacity,
                options,
                quota,
                failure,
                retry,
            },
            flights,
        )
    }
}

impl<'d, ID: Identity> Accept<'d, ID, ()> {
    pub(in crate::network::listener) fn commit(
        self,
        flights: flight::Slots<'d, <ID as route::Materialize<kind::Accept>>::With>,
    ) -> Accept<'d, ID> {
        let Self {
            fd,
            flights: (),
            mode,
            capacity,
            options,
            quota,
            failure,
            retry,
        } = self;
        Accept {
            fd,
            flights,
            mode,
            capacity,
            options,
            quota,
            failure,
            retry,
        }
    }
}

impl<'d, ID: Identity> Accept<'d, ID> {
    pub(in crate::network::listener) const fn failure(&self) -> Option<AcceptError> {
        self.failure
    }

    pub(in crate::network::listener) fn failed(self: pin::Pin<&mut Self>, error: AcceptError) {
        self.project().failure.get_or_insert(error);
    }

    pub(in crate::network::listener) const fn options(&self) -> option::Set {
        self.options
    }

    pub(in crate::network::listener) fn progress(
        &self,
        region: &region::Token<region::Identity<'d>>,
    ) -> schedule::Progress<'d> {
        match self.retry {
            Some(until) => {
                let waiting = schedule::Progress::until(until);
                match &self.mode {
                    mode::Mode::Multishot { arm, .. } => arm.progress(region).reduce(waiting),
                    // Live lanes await native completion; stop clears retry before cancelling.
                    // Idle Ready lanes must not make the cooldown runnable.
                    mode::Mode::Oneshot(_) => waiting,
                }
            }
            None => self.mode.progress(self.fd.driver(), region),
        }
    }

    pub(in crate::network::listener) fn stop_accept(
        self: pin::Pin<&mut Self>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let this = self.project();
        *this.retry = None;
        this.mode.stop(driver);
    }

    fn defer(
        mut self: pin::Pin<&mut Self>,
        now: clock::Moment<'d>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        if self.mode.is_stopped() {
            return;
        }
        *self.as_mut().project().retry = Some(now.clamp_add(RETRY_DELAY));
        self.pause(driver);
    }

    pub(in crate::network::listener) fn pause(
        self: pin::Pin<&mut Self>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        if let mode::Mode::Multishot { arm, interest } = self.project().mode {
            *interest = mode::Interest::Full;
            arm.stop(driver);
        }
    }

    pub(in crate::network::listener) fn is_active(&self) -> bool {
        self.mode.in_flight() != 0
    }

    pub(in crate::network::listener) fn finish(
        self: pin::Pin<&mut Self>,
        proof: &mut finish::Finish<'_, 'd>,
    ) {
        let this = self.project();
        *this.retry = None;
        this.mode.finish(proof);
    }

    pub(in crate::network::listener) fn release(
        self: pin::Pin<&mut Self>,
        admit: Admission<'d, ID>,
    ) {
        self.project().quota.release(admit);
    }

    fn admit(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        ip: net::IpAddr,
    ) -> Result<Option<Admission<'d, ID>>, peers::Full> {
        self.project().quota.admit(key, ip)
    }
}

impl<'d, ID: Identity> Accept<'d, ID> {
    pub(in crate::network::listener) fn arm(
        self: pin::Pin<&mut Self>,
        driver: &mut retained::Context<'_, '_, 'd>,
        slots: capacity::Count,
        work: maint::Work<'_, 'd>,
    ) -> Result<(), result::SubmitError> {
        let slots = slots.get();
        let this = self.project();
        if this.mode.is_stopped() {
            if matches!(
                this.mode.progress(driver.driver_ref(), &driver.regions().0),
                schedule::Progress::Runnable
            ) {
                work.apply(|| {
                    this.mode.stop(driver.driver());
                    Acquire::Item
                });
            }
            return Ok(());
        }
        let ready = match *this.retry {
            Some(until) if !until.is_due(driver.driver_ref().clock().now()) => false,
            Some(_) => {
                *this.retry = None;
                true
            }
            None => true,
        };
        let fd = this.fd.as_ref().get_ref();
        match this.mode {
            mode::Mode::Multishot { arm, interest } => {
                if slots == 0 {
                    *interest = mode::Interest::Full;
                }
                if *interest == mode::Interest::Full {
                    if arm.is_live() {
                        work.apply(|| {
                            arm.stop(driver.driver());
                            Acquire::Item
                        });
                    }
                    if slots == 0 || arm.is_live() || !ready {
                        return Ok(());
                    }
                    arm.restart();
                    *interest = mode::Interest::Ready;
                }
                let sent = work.run(|| {
                    let arming = arm.begin()?;
                    Some(arming.accept(driver.driver(), this.flights, fd))
                });
                if let quota::Admission::Item(arm::Submission::Failed(error)) = sent {
                    return Err(error);
                }
            }
            mode::Mode::Oneshot(lanes) => {
                if !ready {
                    return Ok(());
                }
                let mut left = lanes.demand(slots);
                for _ in 0..lanes.entries.len() {
                    if left == 0 || work.remaining() == 0 {
                        break;
                    }
                    let Some(mode::Turn {
                        ordinal,
                        lane,
                        cursor,
                    }) = lanes.next()
                    else {
                        break;
                    };
                    let admitted = work.run(|| {
                        let arming = lane.begin()?;
                        // SAFETY: the installed listener owner retains this exact
                        // pinned lane through terminal completion or quiescence.
                        Some(unsafe { arming.submit(driver, this.flights, fd) })
                    });
                    match admitted {
                        quota::Admission::Item(arm::Submission::Armed) => {
                            left -= 1;
                        }
                        quota::Admission::Item(arm::Submission::Deferred) => {
                            *cursor = ordinal;
                            break;
                        }
                        quota::Admission::Item(arm::Submission::Failed(error)) => {
                            return Err(error);
                        }
                        quota::Admission::Empty => continue,
                        quota::Admission::Exhausted => break,
                    };
                }
            }
        }
        Ok(())
    }

    pub(in crate::network::listener) fn complete(
        self: pin::Pin<&mut Self>,
        done: accept::Completion<'d>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Incoming<'d> {
        let (token, flow, outcome) = done.into_parts();
        let this = self.project();
        match this.mode {
            mode::Mode::Multishot { arm, .. } => {
                let action = arm.complete(token, flow);
                if action == arm::Disposition::Discard {
                    return close_if_accepted(outcome, driver);
                }
                let slot = match outcome {
                    accept::Outcome::Accepted(slot) => slot,
                    accept::Outcome::Failed(cause) => {
                        return Incoming::Failed {
                            target: token,
                            defer: cause.can_defer(driver),
                            cause,
                            flow,
                        };
                    }
                };
                let fd = slot.bind();
                Incoming::Accepted(fd, None)
            }
            mode::Mode::Oneshot(lanes) => {
                match flow {
                    batch::Flow::More => return close_if_accepted(outcome, driver),
                    batch::Flow::Last => {}
                }
                let Some(mode::Turn {
                    ordinal,
                    lane,
                    cursor,
                }) = lanes.resolve(token.epoch())
                else {
                    return close_if_accepted(outcome, driver);
                };
                let retired = match lane.retire(token) {
                    Retire::Stale => {
                        return close_if_accepted(outcome, driver);
                    }
                    Retire::Done(retired) => retired,
                };
                *cursor = ordinal;
                match (retired, outcome) {
                    (Retired::Terminal(terminal), accept::Outcome::Accepted(slot)) => {
                        let fd = slot.bind();
                        match terminal.into_addr() {
                            Ok(address) => Incoming::Accepted(fd, Some(address.ip())),
                            Err(cause) => Incoming::Invalid(fd, cause),
                        }
                    }
                    (Retired::Terminal(terminal), accept::Outcome::Failed(cause)) => {
                        let defer = terminal.can_defer(&cause, driver);
                        Incoming::Failed {
                            target: token,
                            cause,
                            flow,
                            defer,
                        }
                    }
                    (Retired::Cancelled, outcome) => close_if_accepted(outcome, driver),
                }
            }
        }
    }
}

#[inline]
fn close_if_accepted<'d>(
    outcome: accept::Outcome<'d>,
    driver: &mut driver::Context<'_, 'd>,
) -> Incoming<'d> {
    if let accept::Outcome::Accepted(slot) = outcome {
        slot.bind().close(driver);
    }
    Incoming::Rejected
}
