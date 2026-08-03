use std::hash::BuildHasher;
use std::net::IpAddr;
use std::pin::Pin;

use dope_core::backend::{RawSqe, RetainedSqe, Sqe, StableSqeSource};
use dope_core::driver::submission::Submission;
use dope_core::driver::token::kind::ACCEPT;
use dope_core::driver::token::{SlotIndex, Token};
use dope_core::io::AcceptEvent;
use dope_core::io::fd::Fd;
use dope_core::io::socket::addr::Addr;
use dope_net::Transport;
use dope_net::link::raw::core::Core;
use dope_net::multishot::Multishot;
use o3::collections::FixedHashTable;

use crate::manifold::env::Env;
use crate::manifold::listener::Listener;
use crate::manifold::listener::application::{Application, ApplicationHooks};
use crate::manifold::listener::idle::IdlePhase;
use crate::manifold::listener::state::{EgressCtx, State};
use crate::runtime::dispatcher::FinishContext;
use crate::runtime::profile::RuntimeProfile;
use crate::{DriverContext, hash, manifold};

struct AcceptSubmission(RawSqe);

// SAFETY: this source is built only from Accept's owned peer output and fixed
// fd, both retained until the arm completes or is canceled and quiesced.
unsafe impl StableSqeSource for AcceptSubmission {
    fn into_raw(self) -> RawSqe {
        self.0
    }
}

struct PeerCount {
    ip: IpAddr,
    connections: u32,
}

struct PeerCounts {
    table: FixedHashTable<PeerCount>,
    hash_builder: hash::State,
}

impl PeerCounts {
    fn with_capacity(capacity: usize, hash_builder: hash::State) -> Self {
        Self {
            table: FixedHashTable::with_capacity(capacity),
            hash_builder,
        }
    }

    fn release(&mut self, ip: IpAddr) {
        let hash = self.hash_builder.hash_one(ip);
        let remove = match self.table.get_mut(hash, |count| count.ip == ip) {
            Some(count) if count.connections > 1 => {
                count.connections -= 1;
                false
            }
            Some(_) => true,
            None => false,
        };
        if remove {
            let _ = self.table.remove(hash, |count| count.ip == ip);
        }
    }

    fn acquire(&mut self, ip: IpAddr, limit: u32) -> bool {
        let hash = self.hash_builder.hash_one(ip);
        if let Some(count) = self.table.get_mut(hash, |count| count.ip == ip) {
            if count.connections >= limit {
                return false;
            }
            count.connections += 1;
            return true;
        }
        self.table
            .try_insert(hash, PeerCount { ip, connections: 1 }, |count| {
                count.ip == ip
            })
            .is_ok()
    }
}

enum Outcome<'d> {
    Accepted(Fd<'d>, Option<IpAddr>),
    Capped(IpAddr),
    Rejected,
}

pub(in crate::manifold::listener) struct Accept<'d, T: Transport> {
    fd: Fd<'d>,
    arm: Multishot,
    accept_slot: SlotIndex,
    stream: T::StreamConfig,
    peer_addr: Addr,
    per_ip_limit: u32,
    per_ip_counts: Option<PeerCounts>,
    canceling: bool,
}

impl<'d, T: Transport> Accept<'d, T> {
    pub(in crate::manifold::listener) fn new(
        fd: Fd<'d>,
        accept_slot: SlotIndex,
        stream: T::StreamConfig,
        per_ip_limit: u32,
        hash_builder: hash::State,
    ) -> Self {
        let max_connections = accept_slot.raw();
        Self {
            fd,
            arm: Multishot::default(),
            accept_slot,
            stream,
            peer_addr: Addr::empty(),
            per_ip_limit,
            per_ip_counts: (per_ip_limit != 0)
                .then(|| PeerCounts::with_capacity(max_connections as usize, hash_builder)),
            canceling: false,
        }
    }

    pub(in crate::manifold::listener) fn stream_config(&self) -> &T::StreamConfig {
        &self.stream
    }

    pub(in crate::manifold::listener) fn needs_rearm(&self) -> bool {
        self.arm.needs_rearm()
    }

    pub(in crate::manifold::listener) fn request_rearm(&mut self) {
        self.arm.request_rearm();
    }

    pub(in crate::manifold::listener) fn arm(
        &mut self,
        route: u8,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let Some(ud) = self.arm.begin(route, self.accept_slot) else {
            return;
        };
        self.peer_addr = Addr::empty();
        let source = AcceptSubmission(RawSqe::accept_oneshot(
            &self.fd,
            self.peer_addr.mut_ptr(),
            self.peer_addr.len_ptr(),
            ud,
        ));
        let pushed = driver
            .push_retained(RetainedSqe::from_stable(source))
            .is_ok();
        self.arm.settle(pushed);
    }

    pub(in crate::manifold::listener) fn stop_accept(
        &mut self,
        route: u8,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        if self.arm.is_armed() {
            let token = Token::new(route, self.accept_slot, self.arm.current_epoch());
            let _ = driver.push(Sqe::cancel(token, ACCEPT));
            self.canceling = true;
        }
        self.arm.quiesce();
    }

    pub(in crate::manifold::listener) fn quiesce_target(&self, route: u8) -> Option<Token> {
        (self.arm.is_armed() || self.canceling).then(|| {
            Token::new(route, self.accept_slot, self.arm.current_epoch()).with_kind(ACCEPT)
        })
    }

    pub(in crate::manifold::listener) fn finish(&mut self, context: &mut FinishContext<'_, 'd>) {
        context.retire_fixed_fd(&mut self.fd);
    }

    pub(in crate::manifold::listener) fn release_peer_ip(&mut self, ip: IpAddr) {
        let Some(counts) = self.per_ip_counts.as_mut() else {
            return;
        };
        counts.release(ip);
    }

    fn acquire_peer_ip(&mut self, ip: IpAddr) -> bool {
        let Some(counts) = self.per_ip_counts.as_mut() else {
            return true;
        };
        counts.acquire(ip, self.per_ip_limit)
    }

    fn complete(
        &mut self,
        ud: Token,
        more: bool,
        e: AcceptEvent<'d>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Outcome<'d> {
        let matches = self.arm.epoch_match(ud, self.accept_slot);
        let canceling = self.canceling;
        self.arm.complete(more);
        if canceling && !more {
            self.canceling = false;
        }

        match e {
            AcceptEvent::Failed(_) => Outcome::Rejected,
            AcceptEvent::Accepted(slot) => {
                let fd = slot.bind(self.fd.driver());
                if !matches || canceling || fd.index() >= self.accept_slot.raw() {
                    drop(driver.guard(fd));
                    return Outcome::Rejected;
                }
                let peer_ip = self.peer_addr.into_std().ok().map(|sa| sa.ip());
                if let Some(ip) = peer_ip
                    && !self.acquire_peer_ip(ip)
                {
                    drop(driver.guard(fd));
                    return Outcome::Capped(ip);
                }
                Outcome::Accepted(fd, peer_ip)
            }
        }
    }
}

pub(in crate::manifold::listener) trait AcceptPhase<'d, const ID: u8, A, E>
where
    A: Application<'d>,
    E: Env<Wire = A::Wire>,
{
    fn accept_inherent(
        self: Pin<&mut Self>,
        token: Token,
        more: bool,
        event: AcceptEvent<'d>,
        driver: &mut DriverContext<'_, 'd>,
    );
}

impl<'pool, 'd, const ID: u8, A, E> AcceptPhase<'d, ID, A, E> for Listener<'pool, 'd, ID, A, E>
where
    A: Application<'d>,
    E: Env<Wire = A::Wire>,
{
    fn accept_inherent(
        mut self: Pin<&mut Self>,
        token: Token,
        more: bool,
        e: AcceptEvent<'d>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let (fixed_idx, overflow) = {
            let mut this = self.as_mut().project();
            let (fixed_fd, peer_ip) = match this.accept.complete(token, more, e, driver) {
                Outcome::Accepted(fd, ip) => (fd, ip),
                Outcome::Capped(ip) => {
                    A::Hooks::capped(this.app.as_mut(), ip);
                    return;
                }
                Outcome::Rejected => return,
            };
            let fixed_idx = fixed_fd.token_index();
            let fixed_idx_raw = fixed_idx.raw();
            let conn = this.app.as_ref().connection();
            assert!(
                this.egress_arena
                    .clear(driver.region_token(), fixed_idx_raw as usize),
                "accepted listener lane must be quiescent"
            );
            let conn_slot = State::<A::Conn>::new(conn, peer_ip);
            let _ = <E::Transport as Transport>::submit_quickack(driver, &fixed_fd);
            if !<E::Transport as Transport>::submit_stream_tuning(
                driver,
                *this.accept.stream_config(),
                &fixed_fd,
            ) {
                drop(driver.guard(fixed_fd));
                return;
            }
            let placed = this.pool.insert(
                fixed_idx,
                Core::new(fixed_fd, <E::Transport as Transport>::KERNEL_DISCARD),
                conn_slot,
                driver,
            );
            let placed = match placed {
                Ok(placed) => placed,
                Err(error) => {
                    A::Hooks::open_failed(this.app.as_mut(), &error);
                    return;
                }
            };
            debug_assert!(placed, "accept-direct handed an occupied fixed slot");
            if !placed {
                return;
            }
            this.pool.refresh_wake(fixed_idx);
            let now = driver.turn_now();
            this.idle.arm(fixed_idx, now);
            if E::Profile::ABS_CONN_AGE.is_some() {
                this.idle_abs_age.arm(fixed_idx, now);
            }
            this.pool.arm_recv(fixed_idx, driver);
            let overflow = match this.pool.get_mut(fixed_idx) {
                Some(slot) => {
                    let egress = EgressCtx::for_slot(this.aux, this.egress_arena, fixed_idx);
                    matches!(
                        A::Hooks::accept(this.app.as_mut(), slot, egress, driver),
                        manifold::Outcome::Overrun
                    )
                }
                None => false,
            };
            (fixed_idx, overflow)
        };
        if overflow {
            Self::close_inherent(self.as_mut(), fixed_idx, driver);
        }
    }
}
