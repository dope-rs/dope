use std::{io, net, num, pin, process};

use crate::{
    dispatch::typed::arms,
    listener::{self, connection, handler, runtime::lifecycle::Lifecycle as _},
};

mod peers;
mod sealed;
use dope_core::{
    driver::{
        self, flight, lifecycle, retained,
        route::{self, kind},
        schedule,
    },
    io::{
        event::{accept, tuning},
        fd::handles,
        socket::{self, option},
    },
};
use dope_net::link::pool::{self, ingress::acceptance};
use dope_runtime::random;
use o3::{cell::region, collections::fixed::pinned};
pub(in crate::listener) use sealed::Source;

pub(in crate::listener) enum AcceptOutcome<'d> {
    Accepted(handles::Descriptor<'d>, Option<net::IpAddr>),
    Rejected,
}

#[repr(transparent)]
pub(in crate::listener) struct Admission(net::IpAddr);

type AcceptTag<const ID: u8> = route::KeyTag<ID, { kind::ACCEPT }>;

const MAX_ONESHOT_LANES: usize = schedule::MAX_TURN_WORK_BUDGET;
const _: () = assert!(MAX_ONESHOT_LANES <= u16::MAX as usize);

struct LaneEpochs(num::NonZeroU16);

impl LaneEpochs {
    const NEW: Self = Self(num::NonZeroU16::MIN);

    fn take(&mut self) -> route::Epoch {
        let epoch = self.0;
        self.0 = self.0.saturating_add(1);
        epoch.into()
    }
}

type OneshotArm<'d, const ID: u8> = arms::Arm<'d, AcceptTag<ID>, arms::OneShot>;
type MultishotArm<'d, const ID: u8> = arms::Arm<'d, AcceptTag<ID>, arms::MultiShot>;

#[repr(transparent)]
struct Active<const ID: u8>(usize);

impl<const ID: u8> Active<ID> {
    const ZERO: Self = Self(0);

    fn get(&self) -> usize {
        self.0
    }

    fn activate(&mut self, _armed: arms::Armed<'_, '_, AcceptTag<ID>>) {
        self.0 += 1;
    }

    fn retire<'a, 'd>(
        &mut self,
        retirement: arms::Retirement<'a, 'd, AcceptTag<ID>>,
    ) -> Option<arms::Terminal<'a, 'd, AcceptTag<ID>>> {
        self.0 -= 1;
        retirement.into_terminal()
    }

    fn clear(&mut self) {
        self.0 = 0;
    }
}

const _: () = assert!(std::mem::size_of::<Active<0>>() == std::mem::size_of::<usize>());

#[pin_project::pin_project(!Unpin)]
struct Lane<'d, const ID: u8> {
    arm: OneshotArm<'d, ID>,
    #[pin]
    peer_addr: socket::raw::AcceptAddr,
}

enum Mode<'d, const ID: u8> {
    Multishot {
        arm: MultishotArm<'d, ID>,
        enabled: bool,
    },
    Oneshot(Lanes<'d, ID>),
}

struct Lanes<'d, const ID: u8> {
    entries: pinned::Slice<Lane<'d, ID>>,
    active: Active<ID>,
    target: usize,
    cursor: usize,
    accepting: bool,
}

#[pin_project::pin_project(PinnedDrop, !Unpin)]
pub(in crate::listener) struct Accept<'d, const ID: u8> {
    #[pin]
    fd: handles::Descriptor<'d>,
    flights: flight::Slots<'d, AcceptTag<ID>>,
    mode: Mode<'d, ID>,
    accept_slot: route::SlotIndex,
    options: option::StreamOptions,
    per_ip_limit: u32,
    per_ip_counts: Option<peers::Counts<random::HashState<'d>>>,
}

pub(in crate::listener) struct Prepared<'d> {
    max_connections: usize,
    per_ip_limit: u32,
    per_ip_counts: Option<peers::Counts<random::HashState<'d>>>,
}

impl<'d> Prepared<'d> {
    pub(in crate::listener) fn try_new(
        max_connections: usize,
        per_ip_limit: u32,
        hash_builder: random::HashState<'d>,
    ) -> io::Result<Option<Self>> {
        let per_ip_counts = if per_ip_limit == 0 {
            None
        } else {
            let Some(counts) = peers::Counts::try_with_capacity(max_connections, hash_builder)?
            else {
                return Ok(None);
            };
            Some(counts)
        };
        Ok(Some(Self {
            max_connections,
            per_ip_limit,
            per_ip_counts,
        }))
    }

    pub(in crate::listener) fn flight_capacity(&self) -> usize {
        if self.per_ip_limit == 0 {
            1
        } else {
            self.max_connections.min(MAX_ONESHOT_LANES)
        }
    }

    pub(in crate::listener) fn bind<const ID: u8>(
        self,
        fd: handles::Descriptor<'d>,
        flights: flight::Slots<'d, AcceptTag<ID>>,
        accept_slot: route::SlotIndex,
        options: option::StreamOptions,
    ) -> io::Result<Accept<'d, ID>> {
        let targets = route::Space::<AcceptTag<ID>>::for_driver(fd.driver());
        let mode = if self.per_ip_limit == 0 {
            Mode::Multishot {
                arm: MultishotArm::new(targets.bind(accept_slot, route::Epoch::INITIAL)),
                enabled: true,
            }
        } else {
            let capacity = self.flight_capacity();
            let mut epochs = LaneEpochs::NEW;
            use o3::collections::BoxSliceExt;

            let entries: Box<[Lane<'d, ID>]> = BoxSliceExt::try_box_with(capacity, |_| Lane {
                arm: OneshotArm::new(targets.bind(accept_slot, epochs.take())),
                peer_addr: socket::raw::AcceptAddr::empty(),
            })?;
            Mode::Oneshot(Lanes {
                entries: entries.into(),
                active: Active::ZERO,
                target: capacity,
                cursor: 0,
                accepting: true,
            })
        };
        Ok(Accept {
            fd,
            flights,
            mode,
            accept_slot,
            options,
            per_ip_limit: self.per_ip_limit,
            per_ip_counts: self.per_ip_counts,
        })
    }
}

impl<'d, const ID: u8> Mode<'d, ID> {
    fn progress<'a>(&self, region: &region::Token<'a>) -> schedule::Progress<'a> {
        match self {
            Self::Multishot { arm, enabled } => {
                if *enabled && arm.needs_arm() {
                    schedule::Progress::Runnable
                } else if *enabled || arm.has_in_flight() {
                    arm.progress(region)
                } else {
                    schedule::Progress::Quiescent
                }
            }
            Self::Oneshot(lanes) => {
                if lanes.accepting && lanes.active.get() < lanes.target {
                    schedule::Progress::Runnable
                } else if lanes.active.get() != 0 {
                    schedule::Progress::waiting(region)
                } else {
                    schedule::Progress::Quiescent
                }
            }
        }
    }

    fn in_flight(&self) -> usize {
        match self {
            Self::Multishot { arm, .. } => usize::from(arm.has_in_flight()),
            Self::Oneshot(lanes) => lanes.active.get(),
        }
    }

    fn stop(&mut self, driver: &mut driver::Context<'_, 'd>) {
        match self {
            Self::Multishot { arm, enabled } => {
                *enabled = false;
                arm.stop(driver);
            }
            Self::Oneshot(lanes) => {
                lanes.accepting = false;
                lanes.target = 0;
                for index in 0..lanes.entries.len() {
                    let Some(lane) = lanes.entries.get_mut(index) else {
                        process::abort();
                    };
                    lane.project().arm.stop(driver);
                }
            }
        }
    }

    fn retry_stop(&mut self, driver: &mut driver::Context<'_, 'd>) {
        match self {
            Self::Multishot { arm, .. } => arm.retry_stop(driver),
            Self::Oneshot(lanes) => {
                for index in 0..lanes.entries.len() {
                    let Some(lane) = lanes.entries.get_mut(index) else {
                        process::abort();
                    };
                    lane.project().arm.retry_stop(driver);
                }
            }
        }
    }

    fn finish(&mut self, finish: &mut lifecycle::Finalize<'_, 'd>) {
        match self {
            Self::Multishot { arm, enabled } => {
                arm.finish_quiesced(finish);
                *enabled = false;
            }
            Self::Oneshot(lanes) => {
                for index in 0..lanes.entries.len() {
                    let Some(lane) = lanes.entries.get_mut(index) else {
                        process::abort();
                    };
                    lane.project().arm.finish_quiesced(finish);
                }
                lanes.active.clear();
                lanes.target = 0;
                lanes.accepting = false;
            }
        }
    }
}

impl<'d, const ID: u8> Accept<'d, ID> {
    pub(in crate::listener) const fn options(&self) -> option::StreamOptions {
        self.options
    }

    pub(in crate::listener) fn progress<'a>(
        &self,
        region: &region::Token<'a>,
    ) -> schedule::Progress<'a> {
        self.mode.progress(region)
    }

    pub(in crate::listener) fn stop_accept(
        self: pin::Pin<&mut Self>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        self.project().mode.stop(driver);
    }

    pub(in crate::listener) fn retry_stop(
        self: pin::Pin<&mut Self>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        self.project().mode.retry_stop(driver);
    }

    pub(in crate::listener) fn has_in_flight(&self) -> bool {
        self.mode.in_flight() != 0
    }

    pub(in crate::listener) fn finish(
        self: pin::Pin<&mut Self>,
        finish: &mut lifecycle::Finalize<'_, 'd>,
    ) {
        let this = self.project();
        this.mode.finish(finish);
    }

    pub(in crate::listener) fn release_peer_ip(self: pin::Pin<&mut Self>, admission: Admission) {
        let Some(counts) = self.project().per_ip_counts.as_mut() else {
            return;
        };
        counts.release(admission.0);
    }

    fn admit_peer_ip(self: pin::Pin<&mut Self>, ip: net::IpAddr) -> Option<Admission> {
        let this = self.project();
        let Some(counts) = this.per_ip_counts.as_mut() else {
            return Some(Admission(ip));
        };
        counts
            .acquire(ip, *this.per_ip_limit)
            .then_some(Admission(ip))
    }
}

#[pin_project::pinned_drop]
impl<const ID: u8> PinnedDrop for Accept<'_, ID> {
    fn drop(self: pin::Pin<&mut Self>) {
        if self.project().mode.in_flight() != 0 {
            process::abort();
        }
    }
}

pub(in crate::listener) trait AcceptPhase<'d, const ID: u8, A, E>
where
    A: handler::Application<'d, ID>,
    E: crate::Env<Wire = A::Wire>,
{
    fn accept_inherent(
        self: pin::Pin<&mut Self>,
        token: route::Token,
        completion: accept::Completion<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );

    fn tuning_inherent(
        self: pin::Pin<&mut Self>,
        completion: tuning::Completion,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );

    fn finish_accepted(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );
}

impl<'d, const ID: u8, A, E> AcceptPhase<'d, ID, A, E> for listener::Listener<'d, ID, A, E>
where
    A: handler::Application<'d, ID>,
    E: crate::Env<Wire = A::Wire>,
{
    fn accept_inherent(
        mut self: pin::Pin<&mut Self>,
        token: route::Token,
        completion: accept::Completion<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let accepted = {
            let mut this = self.as_mut().project();
            let (fixed_fd, peer_ip) =
                match Source::complete_source(this.accept.as_mut(), token, completion, driver) {
                    AcceptOutcome::Accepted(fd, ip) => (fd, ip),
                    AcceptOutcome::Rejected => return,
                };
            let fixed_idx = fixed_fd.token_index();
            let mut accept = this.accept.as_mut();
            let app = this.app.as_mut();
            this.owner.pool_mut().accept_with(
                fixed_idx,
                fixed_fd,
                accept.as_ref().options(),
                || {
                    let admission = match peer_ip {
                        Some(ip) => match accept.as_mut().admit_peer_ip(ip) {
                            Some(admission) => Some(admission),
                            None => return Err(ip),
                        },
                        None => None,
                    };
                    Ok(connection::State::<ID, A::Conn>::new(
                        app.as_ref().connection(),
                        admission,
                    ))
                },
                driver,
            )
        };
        match accepted {
            Ok(acceptance::Outcome::Ready(key)) => {
                self.as_mut().finish_accepted(key, turn.reborrow(), driver)
            }
            Ok(acceptance::Outcome::Failed(key)) => {
                self.as_mut().close_slot(key, turn.reborrow(), driver);
            }
            Ok(
                acceptance::Outcome::Pending
                | acceptance::Outcome::Unavailable
                | acceptance::Outcome::Rejected(_),
            )
            | Err(_) => {}
        }
    }

    fn tuning_inherent(
        mut self: pin::Pin<&mut Self>,
        completion: tuning::Completion,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let completed = self
            .as_mut()
            .project()
            .owner
            .pool_mut()
            .tuning()
            .complete(completion);
        let key = match completed {
            acceptance::Completion::Ready(index) => index,
            acceptance::Completion::Failed(index) => {
                self.as_mut().close_slot(index, turn.reborrow(), driver);
                return;
            }
            acceptance::Completion::Stale => return,
        };
        self.as_mut().finish_accepted(key, turn, driver);
    }

    fn finish_accepted(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let overflow = {
            let mut this = self.as_mut().project();
            this.owner.pool().refresh_wake(key);
            let now = driver.turn_now();
            let deadlines_armed =
                this.schedule.inbound.arm(key, now) && this.schedule.absolute.arm(key, now);
            if deadlines_armed {
                this.owner.pool_mut().ingress().arm(key, driver);
                match this.owner.egress_mut(key) {
                    Some(mut egress) => {
                        matches!(
                            A::accept(
                                this.app.as_mut(),
                                egress.context(turn.reborrow().application()),
                                driver,
                            ),
                            crate::Outcome::Overrun
                        )
                    }
                    None => false,
                }
            } else {
                true
            }
        };
        if overflow {
            self.as_mut().close_slot(key, turn, driver);
        }
    }
}
