use std::io::{self, Error, ErrorKind};
use std::marker::PhantomPinned;
use std::net::SocketAddr;
use std::num::NonZeroU16;
use std::pin::Pin;

use pin_project::pin_project;

use dope_core::backend;
use dope_core::driver::bootstrap::Bootstrap;
use dope_core::driver::datagram::Datagram;
use dope_core::driver::route::Route;
use dope_core::io::provided::ProvidedLease;
use o3::collections::FixedQueue;

use crate::DriverContext;
use crate::runtime::dispatcher::FinishContext;

mod raw;
mod send;

pub const MAX_GSO_BYTES: usize = backend::MAX_GSO_BYTES;
pub const MAX_GSO_SEGMENTS: usize = backend::MAX_GSO_SEGMENTS;

use raw::io::Io;
use send::Outgoing;
use send::Payload;
use send::SendOp;

pub struct Packet<'d> {
    guard: ProvidedLease<'d>,
    offset: usize,
    len: usize,
}

impl AsRef<[u8]> for Packet<'_> {
    fn as_ref(&self) -> &[u8] {
        &self.guard.as_slice()[self.offset..self.offset + self.len]
    }
}

impl<'d> Packet<'d> {
    pub fn release(self, driver: &mut DriverContext<'_, 'd>) {
        self.guard.release(driver);
    }
}

pub trait Handler<'d, const ID: u8> {
    fn packet(
        &mut self,
        addr: SocketAddr,
        packet: Packet<'d>,
        sock: Pin<&mut Socket<'d, ID>>,
        driver: &mut DriverContext<'_, 'd>,
    );

    fn empty(&mut self, sock: Pin<&mut Socket<'d, ID>>) {
        let _ = sock;
    }

    fn truncated(&mut self, src: SocketAddr, partial: &[u8], sock: Pin<&mut Socket<'d, ID>>) {
        let _ = (src, partial, sock);
    }

    fn error(&mut self, errno: i32, sock: Pin<&mut Socket<'d, ID>>) {
        let _ = (errno, sock);
    }
}

const RECV_ARM_TAG: SlotIndex = SlotIndex::ZERO;

use dope_core::driver::token::kind::RECV;
use dope_core::driver::token::kind::SEND;
use dope_core::driver::token::{KeyTag, SlotIndex, Token, TokenCapacity, TokenSlab};
use dope_core::io::RecvEvent;
use dope_core::io::SendEvent;
use dope_core::io::datagram::RecvOutcome;
use dope_core::io::fd::Fd;
use dope_core::io::socket::msg::MsgHdr;
use dope_net::multishot::Multishot;
use libc::sockaddr_storage;
use o3::buffer::Lease;
use std::iter::once;

type SendTag<const ID: u8> = KeyTag<ID, { SEND }>;
#[pin_project(!Unpin)]
pub struct Socket<'d, const ID: u8> {
    route: Route<'d, ID>,
    fixed_fd: Fd<'d>,
    bound_addr: SocketAddr,
    recv_arm: Multishot,
    recv_msghdr: MsgHdr,
    pending_outgoing: FixedQueue<Outgoing<'d>>,
    retained_outgoing_bytes: usize,
    in_flight: TokenSlab<SendOp<'d>, SendTag<ID>>,
    #[pin]
    _pin: PhantomPinned,
}

impl<'d, const ID: u8> Socket<'d, ID> {
    const OUT_CAP: usize = 4096;
    const OUT_BYTES_CAP: usize = 16 << 20;
    const IN_FLIGHT_SENDS_CAP: usize = 4096;

    pub fn bind(addr: SocketAddr, driver: &mut DriverContext<'_, 'd>) -> io::Result<Self> {
        let mut msghdr_template = MsgHdr::empty();
        msghdr_template.set_namelen(size_of::<sockaddr_storage>() as u32);
        let mut arm = Multishot::default();
        arm.request_rearm();
        let pending_outgoing = FixedQueue::with_capacity(Self::OUT_CAP);
        let in_flight_capacity =
            TokenCapacity::new(Self::IN_FLIGHT_SENDS_CAP).ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidInput,
                    "dope: datagram capacity exceeds token slots",
                )
            })?;
        let in_flight = TokenSlab::with_capacity(in_flight_capacity);
        let mut route = Route::reserve_transaction(driver)?;
        let (fixed_fd, bound_addr) = route.driver().bind_datagram_slot(addr)?;
        let route = route.commit();
        Ok(Self {
            route,
            fixed_fd,
            bound_addr,
            recv_arm: arm,
            recv_msghdr: msghdr_template,
            pending_outgoing,
            retained_outgoing_bytes: 0,
            in_flight,
            _pin: PhantomPinned,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.bound_addr
    }

    pub fn queue_to(
        self: Pin<&mut Self>,
        payload: Vec<u8>,
        addr: SocketAddr,
    ) -> Result<(), Vec<u8>> {
        if !self.fits(1, payload.len()) {
            return Err(payload);
        }
        let bytes = payload.len();
        self.enqueue_all(bytes, once(Outgoing::plain(Payload::Owned(payload), addr)));
        Ok(())
    }

    pub fn queue_buffer(
        self: Pin<&mut Self>,
        payload: Lease<'d>,
        addr: SocketAddr,
    ) -> Result<(), Lease<'d>> {
        if !self.fits(1, payload.len()) {
            return Err(payload);
        }
        let bytes = payload.len();
        self.enqueue_all(bytes, once(Outgoing::plain(Payload::Buffer(payload), addr)));
        Ok(())
    }

    pub fn queue_packet(
        self: Pin<&mut Self>,
        packet: Packet<'d>,
        addr: SocketAddr,
    ) -> Result<(), Packet<'d>> {
        if !self.fits(1, packet.as_ref().len()) {
            return Err(packet);
        }
        let bytes = packet.as_ref().len();
        self.enqueue_all(bytes, once(Outgoing::plain(Payload::Packet(packet), addr)));
        Ok(())
    }

    pub fn queue_gso(
        self: Pin<&mut Self>,
        payload: Vec<u8>,
        segment_size: NonZeroU16,
        addr: SocketAddr,
    ) -> Result<(), Vec<u8>> {
        let bytes = payload.len();
        let segment_bytes = usize::from(segment_size.get());
        let segments = bytes.div_ceil(segment_bytes);
        if !(2..=MAX_GSO_SEGMENTS).contains(&segments)
            || bytes > MAX_GSO_BYTES
            || !self.fits(1, bytes)
        {
            return Err(payload);
        }
        self.enqueue_all(bytes, once(Outgoing::gso(payload, segment_size, addr)));
        Ok(())
    }

    fn fits(&self, items: usize, bytes: usize) -> bool {
        items <= Self::OUT_CAP - self.pending_outgoing.len()
            && self.retained_outgoing_bytes.saturating_add(bytes) <= Self::OUT_BYTES_CAP
    }

    fn enqueue_all(self: Pin<&mut Self>, bytes: usize, chunks: impl Iterator<Item = Outgoing<'d>>) {
        let this = self.project();
        *this.retained_outgoing_bytes += bytes;
        for chunk in chunks {
            let Some(entry) = this.pending_outgoing.vacant_entry() else {
                unreachable!()
            };
            entry.push_back(chunk);
        }
    }

    pub fn tick(mut self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let needs_rearm = self.recv_arm.needs_rearm();
        let mut io = Io::new(self.as_mut(), driver);
        if needs_rearm {
            io.arm_recv();
        }
        io.flush_outgoing();
    }

    pub fn needs_flush(&self) -> bool {
        !self.pending_outgoing.is_empty() || self.recv_arm.needs_rearm()
    }

    pub fn dispatch_recv<H: Handler<'d, ID>>(
        mut self: Pin<&mut Self>,
        ud: Token,
        more: bool,
        e: RecvEvent<'d>,
        handler: &mut H,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let guard = match e {
            RecvEvent::Data(buffer) => buffer,
            RecvEvent::Failed(errno) => {
                handler.error(errno, self);
                return;
            }
            RecvEvent::Eof
            | RecvEvent::Cancelled
            | RecvEvent::Starved
            | RecvEvent::Discarded { .. } => return,
        };
        let msghdr = {
            let this = self.as_mut().project();
            if !this.recv_arm.epoch_match(ud, RECV_ARM_TAG) {
                return;
            }
            this.recv_arm.complete(more);
            this.recv_msghdr.raw()
        };
        let outcome = driver.driver_ref().recv_packet(&guard, msghdr);
        match outcome {
            RecvOutcome::Packet { src, payload } => {
                let len = payload.len();
                handler.packet(
                    src,
                    Packet {
                        guard,
                        offset: payload.start,
                        len,
                    },
                    self,
                    driver,
                )
            }
            RecvOutcome::Empty => {
                handler.empty(self);
                guard.release(driver);
            }
            RecvOutcome::Truncated { src, partial } => {
                handler.truncated(src, &guard.as_slice()[partial], self);
                guard.release(driver);
            }
            RecvOutcome::Error(errno) => {
                handler.error(errno, self);
                guard.release(driver);
            }
        }
    }

    pub fn dispatch_send<H: Handler<'d, ID>>(
        mut self: Pin<&mut Self>,
        ud: Token,
        e: SendEvent,
        handler: &mut H,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let this = self.as_mut().project();
        if let Some(parts) = ud.parts::<SendTag<ID>>()
            && let Some(op) = this.in_flight.remove_parts(parts)
            && let Some(released) = op.finish(driver)
        {
            debug_assert!(*this.retained_outgoing_bytes >= released);
            *this.retained_outgoing_bytes -= released;
        }
        if let SendEvent::Failed(errno) = e {
            handler.error(errno, self);
        }
    }

    pub fn shutdown(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let this = self.project();
        let mut quiesce = driver.quiesce_batch();
        if this.recv_arm.is_armed() {
            quiesce.cancel(
                Token::new(ID, RECV_ARM_TAG, this.recv_arm.current_epoch()).with_kind(RECV),
            );
        }
        for index in this.in_flight.capacity().slots() {
            if let Some(key) = this.in_flight.key(index.raw()) {
                quiesce.cancel(Token::from_key(key));
            }
        }
        let outcome = quiesce.finish();
        this.route.finish(driver, outcome.has_targets());
    }

    pub fn finish(self: Pin<&mut Self>, context: &mut FinishContext<'_, 'd>) {
        context.retire_fixed_fd(self.project().fixed_fd);
    }
}
