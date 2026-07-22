use std::marker::PhantomPinned;
use std::net::SocketAddr;
use std::pin::Pin;
use std::rc::Rc;
use std::{io, iter};

use pin_project::pin_project;

use dope_core::driver::bootstrap::Bootstrap;
use dope_core::driver::buffers::ProvidedBuffers;
use dope_core::driver::control::ContextControl;
use dope_core::driver::datagram::Datagram;
use dope_core::driver::route::Route;
use dope_core::driver::submission::Submission;
use dope_core::io::provided::ProvidedLease;
use o3::collections::FixedQueue;

use crate::DriverContext;

mod send;

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

const RECV_ARM_TAG: SlotIndex = SlotIndex::new(0);

use dope_core::backend::Sqe;
use dope_core::backend::{MAX_GSO_BYTES, MAX_GSO_SEGMENTS};
use dope_core::driver::token::{KeyTag, SLOT_MASK, SlotIndex, Token, TokenSlab, kind};
use dope_core::io::RecvEvent;
use dope_core::io::SendEvent;
use dope_core::io::datagram::RecvOutcome;
use dope_core::io::fd::Fd;
use dope_core::io::socket::msg::MsgHdr;
use dope_net::multishot::Multishot;

type SendTag<const ID: u8> = KeyTag<ID, { kind::SEND }>;
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
    const IN_FLIGHT_SENDS_CAP: usize = {
        assert!(4096 <= SLOT_MASK as usize + 1);
        4096
    };

    pub fn bind(addr: SocketAddr, driver: &mut DriverContext<'_, 'd>) -> io::Result<Self> {
        let route = Route::reserve(driver)?;
        let (fixed_fd, bound_addr) = driver.bind_datagram_slot(addr)?;
        let mut msghdr_template = MsgHdr::empty();
        msghdr_template.set_namelen(size_of::<libc::sockaddr_storage>() as u32);
        let mut arm = Multishot::default();
        arm.request_rearm();
        Ok(Self {
            route,
            fixed_fd,
            bound_addr,
            recv_arm: arm,
            recv_msghdr: msghdr_template,
            pending_outgoing: FixedQueue::with_capacity(Self::OUT_CAP),
            retained_outgoing_bytes: 0,
            in_flight: TokenSlab::with_capacity(Self::IN_FLIGHT_SENDS_CAP),
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
        self.enqueue_all(
            bytes,
            iter::once(Outgoing::plain(Payload::Owned(payload), addr)),
        );
        Ok(())
    }

    pub fn queue_buffer(
        self: Pin<&mut Self>,
        payload: o3::buffer::Lease<'d>,
        addr: SocketAddr,
    ) -> Result<(), o3::buffer::Lease<'d>> {
        if !self.fits(1, payload.len()) {
            return Err(payload);
        }
        let bytes = payload.len();
        self.enqueue_all(
            bytes,
            iter::once(Outgoing::plain(Payload::Buffer(payload), addr)),
        );
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
        self.enqueue_all(
            bytes,
            iter::once(Outgoing::plain(Payload::Packet(packet), addr)),
        );
        Ok(())
    }

    pub fn queue_segmented(
        self: Pin<&mut Self>,
        payload: Vec<u8>,
        addr: SocketAddr,
        segment_size: u16,
    ) -> Result<(), Vec<u8>> {
        if segment_size == 0 || payload.len() <= segment_size as usize {
            return self.queue_to(payload, addr);
        }
        self.enqueue_segmented(payload, addr, segment_size)
    }

    pub fn queue_segments(
        self: Pin<&mut Self>,
        payload: Vec<u8>,
        segments: &[u32],
        addr: SocketAddr,
    ) -> Result<(), Vec<u8>> {
        let mut items = 0;
        let Some(bytes) = Outgoing::visit_segments(segments, |_, _, _| items += 1) else {
            return Err(payload);
        };
        if items == 0 || bytes != payload.len() || !self.fits(items, bytes) {
            return Err(payload);
        }
        let this = self.project();
        let batch = Rc::new(payload);
        *this.retained_outgoing_bytes += bytes;
        let _ = Outgoing::visit_segments(segments, |offset, len, segment_size| {
            let Some(entry) = this.pending_outgoing.vacant_entry() else {
                unreachable!()
            };
            entry.push_back(Outgoing::range(
                Rc::clone(&batch),
                offset,
                len,
                addr,
                segment_size,
            ));
        });
        Ok(())
    }

    fn enqueue_segmented(
        self: Pin<&mut Self>,
        payload: Vec<u8>,
        addr: SocketAddr,
        seg: u16,
    ) -> Result<(), Vec<u8>> {
        let seg_len = seg as usize;
        let max_send = (MAX_GSO_BYTES / seg_len).clamp(1, MAX_GSO_SEGMENTS) * seg_len;
        if payload.len() <= max_send {
            if !self.fits(1, payload.len()) {
                return Err(payload);
            }
            let bytes = payload.len();
            self.enqueue_all(bytes, iter::once(Outgoing::segmented(payload, addr, seg)));
            return Ok(());
        }
        let items = payload.len().div_ceil(max_send);
        let bytes = payload.len();
        if !self.fits(items, bytes) {
            return Err(payload);
        }
        self.enqueue_segmented_ranges(payload, addr, seg, max_send);
        Ok(())
    }

    fn enqueue_segmented_ranges(
        self: Pin<&mut Self>,
        payload: Vec<u8>,
        addr: SocketAddr,
        segment_size: u16,
        chunk_size: usize,
    ) {
        let bytes = payload.len();
        let this = self.project();
        let batch = Rc::new(payload);
        *this.retained_outgoing_bytes += bytes;
        for offset in (0..bytes).step_by(chunk_size) {
            let len = chunk_size.min(bytes - offset);
            let gso = (len > segment_size as usize).then_some(segment_size);
            let Some(entry) = this.pending_outgoing.vacant_entry() else {
                unreachable!()
            };
            entry.push_back(Outgoing::range(Rc::clone(&batch), offset, len, addr, gso));
        }
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
        if self.recv_arm.needs_rearm() {
            self.as_mut().arm_recv(driver);
        }
        self.flush_outgoing(driver);
    }

    pub fn has_pending(&self) -> bool {
        !self.in_flight.is_empty()
            || !self.pending_outgoing.is_empty()
            || self.recv_arm.needs_rearm()
    }

    pub fn needs_flush(&self) -> bool {
        !self.pending_outgoing.is_empty() || self.recv_arm.needs_rearm()
    }

    pub fn retained_outgoing_bytes(&self) -> usize {
        self.retained_outgoing_bytes
    }

    pub fn dispatch_recv<H: Handler<'d, ID>>(
        mut self: Pin<&mut Self>,
        ud: Token,
        more: bool,
        e: dope_core::io::RecvEvent<'d>,
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
        e: dope_core::io::SendEvent,
        handler: &mut H,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let this = self.as_mut().project();
        if let Some(parts) = ud.parts::<SendTag<ID>>()
            && let Some(op) = this.in_flight.remove_parts(parts.slab())
            && let Some(released) = op.finish(driver)
        {
            debug_assert!(*this.retained_outgoing_bytes >= released);
            *this.retained_outgoing_bytes -= released;
        }
        if let SendEvent::Failed(errno) = e {
            handler.error(errno, self);
        }
    }

    fn arm_recv(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let this = self.project();
        let Some(ud) = this.recv_arm.begin(ID, RECV_ARM_TAG) else {
            return;
        };
        let buf_group = driver.buffer_group();
        let sqe = Sqe::recv_msg_multi(this.fixed_fd, this.recv_msghdr.raw(), buf_group, ud);
        let pushed = driver.push(sqe).is_ok();
        this.recv_arm.settle(pushed);
    }

    fn flush_outgoing(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let this = self.project();
        while this.in_flight.len() < this.in_flight.capacity() {
            let Some(out) = this.pending_outgoing.pop_front() else {
                break;
            };
            let op = SendOp::new(out);
            let (key, msghdr) = match this.in_flight.insert_entry(op) {
                Ok((key, op)) => (key, op.fill_msghdr()),
                Err(op) => {
                    let out = op.into_outgoing();
                    let Some(entry) = this.pending_outgoing.vacant_entry() else {
                        unreachable!()
                    };
                    entry.push_front(out);
                    break;
                }
            };
            let ud = Token::from_key(key);
            let pushed = driver
                .push(Sqe::send_msg(this.fixed_fd, msghdr.raw(), ud))
                .is_ok();
            if !pushed {
                if let Some(op) = this.in_flight.remove(key) {
                    let out = op.into_outgoing();
                    let Some(entry) = this.pending_outgoing.vacant_entry() else {
                        unreachable!()
                    };
                    entry.push_front(out);
                }
                break;
            }
        }
    }

    pub fn shutdown(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let this = self.project();
        let mut targets = Vec::new();
        if this.recv_arm.is_armed() {
            targets.push(
                Token::new(ID, RECV_ARM_TAG, this.recv_arm.current_epoch()).with_kind(kind::RECV),
            );
        }
        for index in 0..this.in_flight.capacity() as u32 {
            if let Some(key) = this.in_flight.key(index) {
                targets.push(Token::from_key(key));
            }
        }
        if !targets.is_empty() {
            driver.quiesce(&targets);
        }
        this.route.finish(driver, !targets.is_empty());
    }
}
