use std::collections::HashMap;
use std::net::IpAddr;

use crate::transport::Transport;
use crate::transport::multishot::Arm;
use crate::{Drive, Driver, backend};

pub enum Outcome<'d> {
    Accepted(backend::socket::Fd<'d>, Option<IpAddr>),
    Capped(IpAddr),
    Rejected,
}

pub struct Accept<'d, T: Transport> {
    fd: backend::socket::Fd<'d>,
    arm: Arm,
    accept_slot: backend::token::LocalIdx,
    stream_opts: T::StreamOpts,
    peer_addr: backend::socket::Addr,
    per_ip_cap: u32,
    per_ip_counts: HashMap<IpAddr, u32>,
}

impl<'d, T: Transport> Accept<'d, T> {
    pub fn new(
        fd: backend::socket::Fd<'d>,
        max_conn: u32,
        stream_opts: T::StreamOpts,
        per_ip_cap: u32,
    ) -> Self {
        Self {
            fd,
            arm: Arm::default(),
            accept_slot: backend::token::LocalIdx::new(max_conn),
            stream_opts,
            peer_addr: backend::socket::Addr::empty(),
            per_ip_cap,
            per_ip_counts: HashMap::new(),
        }
    }

    pub fn stream_opts(&self) -> &T::StreamOpts {
        &self.stream_opts
    }

    pub fn needs_rearm(&self) -> bool {
        self.arm.needs_rearm()
    }

    pub fn request_rearm(&mut self) {
        self.arm.request_rearm();
    }

    pub fn arm(&mut self, route: u8, driver: &'d Driver) {
        let Some(ud) = self.arm.begin(route, self.accept_slot) else {
            return;
        };
        self.peer_addr = backend::socket::Addr::empty();
        let pushed = driver
            .push(backend::sqe::Sqe::accept_oneshot(
                &self.fd,
                self.peer_addr.mut_ptr(),
                self.peer_addr.len_ptr(),
                ud,
            ))
            .is_ok();
        self.arm.settle(pushed);
    }

    pub fn stop_accept(&mut self, route: u8, driver: &'d Driver) {
        if self.arm.is_armed() {
            let token =
                backend::token::Token::new(route, self.accept_slot, self.arm.current_epoch());
            let _ = driver.push(backend::sqe::Sqe::cancel(
                token,
                backend::token::kind::ACCEPT,
            ));
        }
        self.arm.quiesce();
    }

    pub fn release_peer_ip(&mut self, ip: IpAddr) {
        if let std::collections::hash_map::Entry::Occupied(mut e) = self.per_ip_counts.entry(ip) {
            let v = e.get_mut();
            *v = v.saturating_sub(1);
            if *v == 0 {
                e.remove();
            }
        }
    }

    pub fn on_completion(
        &mut self,
        ud: backend::token::Token,
        more: bool,
        e: backend::AcceptEvent,
        driver: &'d Driver,
    ) -> Outcome<'d> {
        if !self.arm.epoch_match(ud, self.accept_slot) {
            return Outcome::Rejected;
        }
        self.arm.on_completion(more);

        match e {
            backend::AcceptEvent::Failed => Outcome::Rejected,
            backend::AcceptEvent::Accepted(slot) => {
                let fd = backend::socket::Fd::adopt(slot, driver);
                if fd.index() >= self.accept_slot.raw() {
                    return Outcome::Rejected;
                }
                let peer_ip = self.peer_addr.to_std().ok().map(|sa| sa.ip());
                if self.per_ip_cap > 0
                    && let Some(ip) = peer_ip
                {
                    let count = self.per_ip_counts.entry(ip).or_insert(0);
                    if *count >= self.per_ip_cap {
                        return Outcome::Capped(ip);
                    }
                    *count += 1;
                }
                Outcome::Accepted(fd, peer_ip)
            }
        }
    }
}
