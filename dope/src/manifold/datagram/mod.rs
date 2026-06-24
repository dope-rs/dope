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
    pending_outgoing: VecDeque<(SocketAddr, Vec<u8>)>,
    pending_outgoing_bytes: usize,
    in_flight: Slab<Box<SendOp>>,
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
            in_flight: Slab::new(Self::IN_FLIGHT_SENDS_CAP),
            _pin: PhantomPinned,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.bound_addr
    }

    pub fn queue_to(self: Pin<&mut Self>, payload: Vec<u8>, addr: SocketAddr) -> bool {
        let this = self.project();
        if this.pending_outgoing.len() >= Self::OUT_CAP
            || this.pending_outgoing_bytes.saturating_add(payload.len()) > Self::OUT_BYTES_CAP
        {
            return false;
        }
        *this.pending_outgoing_bytes += payload.len();
        this.pending_outgoing.push_back((addr, payload));
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
            | backend::RecvEvent::Starved => return,
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
            let Some((addr, payload)) = this.pending_outgoing.pop_front() else {
                break;
            };
            *this.pending_outgoing_bytes =
                this.pending_outgoing_bytes.saturating_sub(payload.len());
            let Some(key) = this.in_flight.alloc(Box::new(SendOp::new(payload, addr))) else {
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

struct SendOp {
    buf: Vec<u8>,
    addr: backend::socket::Addr,
    iov: backend::socket::IoVec,
    msg: backend::socket::MsgHdr,
}

impl SendOp {
    fn new(buf: Vec<u8>, dst: SocketAddr) -> Self {
        Self {
            buf,
            addr: backend::socket::Addr::from_std(dst),
            iov: backend::socket::IoVec::empty(),
            msg: backend::socket::MsgHdr::empty(),
        }
    }

    fn fill_msghdr(&mut self) -> &backend::socket::MsgHdr {
        self.iov = backend::socket::IoVec::from_slice(&self.buf);
        self.msg.set_name(&mut self.addr);
        self.msg.set_iov(slice::from_ref(&self.iov));
        &self.msg
    }
}
