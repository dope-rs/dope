use std::collections::VecDeque;
use std::marker::PhantomPinned;
use std::net::SocketAddr;
use std::pin::Pin;
use std::{io, slice};

use pin_project::pin_project;

use crate::backend;

pub trait Handler<const ID: u8> {
    fn on_packet(&mut self, addr: SocketAddr, data: &[u8], sock: Pin<&mut Socket<ID>>);

    fn on_empty(&mut self, sock: Pin<&mut Socket<ID>>) {
        let _ = sock;
    }

    fn on_truncated(&mut self, src: SocketAddr, partial: &[u8], sock: Pin<&mut Socket<ID>>) {
        let _ = (src, partial, sock);
    }

    fn on_error(&mut self, errno: i32, sock: Pin<&mut Socket<ID>>) {
        let _ = (errno, sock);
    }
}

const RECV_ARM_TAG: backend::token::LocalIdx = backend::token::LocalIdx::new(0);

use crate::slab::Slab;
use crate::transport::multishot::Arm;
use crate::{Bootstrap, Drive, Driver, Lend};

#[pin_project]
pub struct Socket<const ID: u8> {
    fixed_fd: backend::socket::Fd,
    bound_addr: SocketAddr,
    recv_arm: Arm,
    recv_msghdr: backend::socket::MsgHdr,
    pending_outgoing: VecDeque<Outgoing>,
    pending_outgoing_bytes: usize,
    in_flight: Slab<SendOp>,
    #[pin]
    _pin: PhantomPinned,
}

impl<const ID: u8> Socket<ID> {
    const OUT_CAP: usize = 4096;
    const OUT_BYTES_CAP: usize = 16 << 20;
    const IN_FLIGHT_SENDS_CAP: usize = 4096;

    pub fn bind(addr: SocketAddr, driver: &mut Driver) -> io::Result<Self> {
        let (fixed_fd, bound_addr) = driver.bind_datagram_slot(addr)?;
        let mut msghdr_template = backend::socket::MsgHdr::empty();
        msghdr_template.set_namelen(size_of::<libc::sockaddr_storage>() as u32);
        let mut arm = Arm::default();
        arm.request_rearm();
        Ok(Self {
            fixed_fd,
            bound_addr,
            recv_arm: arm,
            recv_msghdr: msghdr_template,
            pending_outgoing: VecDeque::new(),
            pending_outgoing_bytes: 0,
            in_flight: Slab::pinned(Self::IN_FLIGHT_SENDS_CAP),
            _pin: PhantomPinned,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.bound_addr
    }

    pub fn queue_to(self: Pin<&mut Self>, payload: Vec<u8>, addr: SocketAddr) -> bool {
        self.enqueue(Outgoing::plain(payload, addr))
    }

    /// Queues `payload` for UDP GSO: the kernel slices it into `segment_size`-byte
    /// datagrams, only the last shorter. A run past the 64-segment / 65535-byte
    /// cap is chunked across sends; off Linux, into per-segment plain sends.
    /// All-or-nothing.
    ///
    /// ```no_run
    /// # use dope::manifold::datagram::Socket;
    /// # fn f<const ID: u8>(sock: std::pin::Pin<&mut Socket<ID>>, wire: Vec<u8>, dst: std::net::SocketAddr) {
    /// sock.queue_segmented(wire, dst, 1200); // back-to-back 1200B packets -> one sendmsg
    /// # }
    /// ```
    pub fn queue_segmented(
        self: Pin<&mut Self>,
        payload: Vec<u8>,
        addr: SocketAddr,
        segment_size: u16,
    ) -> bool {
        if segment_size == 0 || payload.len() <= segment_size as usize {
            return self.queue_to(payload, addr);
        }
        self.enqueue_segmented(payload, addr, segment_size)
    }

    #[cfg(target_os = "linux")]
    fn enqueue_segmented(
        self: Pin<&mut Self>,
        payload: Vec<u8>,
        addr: SocketAddr,
        seg: u16,
    ) -> bool {
        let seg_len = seg as usize;
        let max_send = (MAX_GSO_BYTES / seg_len).clamp(1, MAX_GSO_SEGMENTS) * seg_len;
        if payload.len() <= max_send {
            return self.enqueue(Outgoing::segmented(payload, addr, seg));
        }
        let items = payload.len().div_ceil(max_send);
        let bytes = payload.len();
        let chunks = payload.chunks(max_send).map(|chunk| {
            let chunk = chunk.to_vec();
            if chunk.len() > seg_len {
                Outgoing::segmented(chunk, addr, seg)
            } else {
                Outgoing::plain(chunk, addr)
            }
        });
        self.enqueue_all(items, bytes, chunks)
    }

    #[cfg(not(target_os = "linux"))]
    fn enqueue_segmented(
        self: Pin<&mut Self>,
        payload: Vec<u8>,
        addr: SocketAddr,
        seg: u16,
    ) -> bool {
        let seg_len = seg as usize;
        let items = payload.len().div_ceil(seg_len);
        let bytes = payload.len();
        let chunks = payload
            .chunks(seg_len)
            .map(|chunk| Outgoing::plain(chunk.to_vec(), addr));
        self.enqueue_all(items, bytes, chunks)
    }

    fn fits(&self, items: usize, bytes: usize) -> bool {
        self.pending_outgoing.len() + items <= Self::OUT_CAP
            && self.pending_outgoing_bytes.saturating_add(bytes) <= Self::OUT_BYTES_CAP
    }

    fn enqueue(self: Pin<&mut Self>, out: Outgoing) -> bool {
        self.enqueue_all(1, out.payload.len(), std::iter::once(out))
    }

    fn enqueue_all(
        self: Pin<&mut Self>,
        items: usize,
        bytes: usize,
        chunks: impl Iterator<Item = Outgoing>,
    ) -> bool {
        if !self.fits(items, bytes) {
            return false;
        }
        let this = self.project();
        *this.pending_outgoing_bytes += bytes;
        this.pending_outgoing.extend(chunks);
        true
    }

    pub fn tick(mut self: Pin<&mut Self>, driver: &mut Driver) {
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

    pub fn dispatch_recv<H: Handler<ID>>(
        mut self: Pin<&mut Self>,
        ud: backend::token::Token,
        more: bool,
        e: backend::RecvEvent,
        driver: &mut Driver,
        handler: &mut H,
    ) {
        let msghdr = {
            let this = self.as_mut().project();
            if !this.recv_arm.epoch_match(ud, RECV_ARM_TAG) {
                if let backend::RecvEvent::Data { bid, .. } = &e {
                    driver.release(Some(*bid));
                }
                return;
            }
            this.recv_arm.on_completion(more);
            this.recv_msghdr.raw()
        };
        let (len, bid) = match e {
            backend::RecvEvent::Data { len, bid } => (len, bid),
            backend::RecvEvent::Failed(errno) => {
                handler.on_error(errno, self);
                return;
            }
            backend::RecvEvent::Eof
            | backend::RecvEvent::Cancelled
            | backend::RecvEvent::Starved
            | backend::RecvEvent::Discarded { .. } => return,
        };
        let (outcome, _guard) = backend::Datagram::recv_packet(driver, len, bid, msghdr);
        match outcome {
            backend::datagram::Outcome::Packet { src, payload } => {
                handler.on_packet(src, payload, self)
            }
            backend::datagram::Outcome::Empty => handler.on_empty(self),
            backend::datagram::Outcome::Truncated { src, partial } => {
                handler.on_truncated(src, partial, self)
            }
            backend::datagram::Outcome::Error(errno) => handler.on_error(errno, self),
        }
    }

    pub fn dispatch_send<H: Handler<ID>>(
        mut self: Pin<&mut Self>,
        ud: backend::token::Token,
        e: backend::SendEvent,
        handler: &mut H,
    ) {
        self.as_mut().project().in_flight.remove(ud.key());
        if let backend::SendEvent::Failed(errno) = e {
            handler.on_error(errno, self);
        }
    }

    fn arm_recv(self: Pin<&mut Self>, driver: &mut Driver) {
        let this = self.project();
        let Some(ud) = this.recv_arm.begin(ID, RECV_ARM_TAG) else {
            return;
        };
        let buf_group = driver.group();
        let sqe =
            backend::sqe::Sqe::recv_msg_multi(this.fixed_fd, this.recv_msghdr.raw(), buf_group, ud);
        let pushed = driver.push(sqe).is_ok();
        this.recv_arm.settle(pushed);
    }

    fn flush_outgoing(self: Pin<&mut Self>, driver: &mut Driver) {
        let this = self.project();
        while this.in_flight.len() < this.in_flight.slot_count() {
            let Some(out) = this.pending_outgoing.pop_front() else {
                break;
            };
            *this.pending_outgoing_bytes = this
                .pending_outgoing_bytes
                .saturating_sub(out.payload.len());
            let Some(key) = this.in_flight.alloc(SendOp::new(out)) else {
                break;
            };
            let msghdr_ref = this.in_flight.get_mut(key).unwrap().fill_msghdr();
            let ud = backend::token::Token::from_key(ID, key);
            let pushed = driver
                .push(backend::sqe::Sqe::send_msg(
                    this.fixed_fd,
                    msghdr_ref.raw(),
                    ud,
                ))
                .is_ok();
            if !pushed {
                this.in_flight.remove(key);
                break;
            }
        }
    }
}

/// One `UDP_SEGMENT` send caps at 64 segments and a 65535-byte payload (Linux).
#[cfg(target_os = "linux")]
const MAX_GSO_SEGMENTS: usize = 64;
#[cfg(target_os = "linux")]
const MAX_GSO_BYTES: usize = 65535;

struct Outgoing {
    addr: SocketAddr,
    payload: Vec<u8>,
    /// UDP_SEGMENT size; 0 = one plain datagram.
    #[cfg(target_os = "linux")]
    segment_size: u16,
}

impl Outgoing {
    fn plain(payload: Vec<u8>, addr: SocketAddr) -> Self {
        Self {
            addr,
            payload,
            #[cfg(target_os = "linux")]
            segment_size: 0,
        }
    }

    #[cfg(target_os = "linux")]
    fn segmented(payload: Vec<u8>, addr: SocketAddr, segment_size: u16) -> Self {
        Self {
            addr,
            payload,
            segment_size,
        }
    }
}

#[cfg(target_os = "linux")]
#[repr(C, align(8))]
struct CmsgBuf([u8; 32]);

struct SendOp {
    buf: Vec<u8>,
    addr: backend::socket::InetAddr,
    iov: backend::socket::IoVec,
    msg: backend::socket::MsgHdr,
    #[cfg(target_os = "linux")]
    segment_size: u16,
    #[cfg(target_os = "linux")]
    cmsg: CmsgBuf,
}

impl SendOp {
    fn new(out: Outgoing) -> Self {
        Self {
            buf: out.payload,
            addr: backend::socket::InetAddr::from_std(out.addr),
            iov: backend::socket::IoVec::empty(),
            msg: backend::socket::MsgHdr::empty(),
            #[cfg(target_os = "linux")]
            segment_size: out.segment_size,
            #[cfg(target_os = "linux")]
            cmsg: CmsgBuf([0; 32]),
        }
    }

    fn fill_msghdr(&mut self) -> &backend::socket::MsgHdr {
        self.iov = backend::socket::IoVec::from_slice(&self.buf);
        let name_ptr = self.addr.mut_ptr();
        let name_len = self.addr.socklen();
        self.msg.set_name_ptr(name_ptr.cast(), name_len);
        self.msg.set_iov(slice::from_ref(&self.iov));
        #[cfg(target_os = "linux")]
        if self.segment_size > 0 {
            self.fill_gso_cmsg();
        }
        &self.msg
    }

    #[cfg(target_os = "linux")]
    fn fill_gso_cmsg(&mut self) {
        const DATA_LEN: u32 = size_of::<u16>() as u32;
        let seg = self.segment_size;
        let (ptr, cap) = (self.cmsg.0.as_mut_ptr(), self.cmsg.0.len());
        unsafe {
            let controllen = libc::CMSG_SPACE(DATA_LEN) as usize;
            debug_assert!(controllen <= cap);
            self.msg.set_control(ptr.cast(), controllen);
            let hdr = libc::CMSG_FIRSTHDR(self.msg.raw());
            (*hdr).cmsg_level = libc::SOL_UDP;
            (*hdr).cmsg_type = libc::UDP_SEGMENT;
            (*hdr).cmsg_len = libc::CMSG_LEN(DATA_LEN) as _;
            std::ptr::copy_nonoverlapping(
                std::ptr::addr_of!(seg).cast::<u8>(),
                libc::CMSG_DATA(hdr),
                DATA_LEN as usize,
            );
        }
    }
}
