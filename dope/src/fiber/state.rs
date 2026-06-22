use std::collections::VecDeque;
use std::io;
use std::task::Waker;

use o3::buffer::Shared;

use crate::backend;

const RECV_QUEUE_CAP: usize = 256;
const RECV_CAP_BYTES: usize = 1 << 20;

pub enum RecvInto {
    Bytes(usize),
    Failed(io::Error),
    Pending,
}

pub enum SendIdle {
    Idle,
    Failed(io::Error),
    Pending,
}

#[derive(Default)]
enum Status {
    #[default]
    Open,
    Closed,
    Failed(io::Error),
}

#[derive(Default)]
pub struct State {
    recv_queue: VecDeque<Shared>,
    recv_queued_bytes: usize,
    status: Status,
    recv_waiter: Option<backend::park::WakeRef>,
    send_waiter: Option<backend::park::WakeRef>,
}

impl State {
    fn wake_waiter(waiter: &mut Option<backend::park::WakeRef>) {
        if let Some(w) = waiter.take() {
            w.wake();
        }
    }

    pub(super) fn push_recv(&mut self, bytes: &[u8]) -> bool {
        if bytes.is_empty() {
            return false;
        }
        let over_cap = self.recv_queue.len() >= RECV_QUEUE_CAP
            || self.recv_queued_bytes + bytes.len() > RECV_CAP_BYTES;
        if self.is_closed() || over_cap {
            if !self.is_closed() {
                self.signal_error(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "fiber: recv backpressure exceeded",
                ));
            }
            return true;
        }
        self.recv_queued_bytes += bytes.len();
        self.recv_queue.push_back(Shared::copy_from_slice(bytes));
        Self::wake_waiter(&mut self.recv_waiter);
        false
    }

    pub(super) fn wake_send(&mut self) {
        Self::wake_waiter(&mut self.send_waiter);
    }

    pub(super) fn signal_error(&mut self, e: io::Error) {
        self.status = Status::Failed(e);
        Self::wake_waiter(&mut self.recv_waiter);
        Self::wake_waiter(&mut self.send_waiter);
    }

    pub(super) fn signal_closed(&mut self) {
        if matches!(self.status, Status::Open) {
            self.status = Status::Closed;
        }
        Self::wake_waiter(&mut self.recv_waiter);
        Self::wake_waiter(&mut self.send_waiter);
    }

    fn is_closed(&self) -> bool {
        !matches!(self.status, Status::Open)
    }

    fn take_error(&mut self) -> Option<io::Error> {
        if !matches!(self.status, Status::Failed(_)) {
            return None;
        }
        let Status::Failed(e) = std::mem::replace(&mut self.status, Status::Closed) else {
            return None;
        };
        Some(e)
    }

    fn arm_waiter(waiter: &mut Option<backend::park::WakeRef>, w: &Waker) {
        *waiter = Some(backend::park::WakeRef::verified(w));
    }

    pub(super) fn set_recv_waker(&mut self, w: &Waker) {
        Self::arm_waiter(&mut self.recv_waiter, w);
    }

    pub(super) fn set_send_waker(&mut self, w: &Waker) {
        Self::arm_waiter(&mut self.send_waiter, w);
    }

    pub(super) fn try_recv_into(&mut self, dst: &mut [u8]) -> RecvInto {
        let filled = self.drain_into(dst);
        if filled > 0 {
            return RecvInto::Bytes(filled);
        }
        if let Some(e) = self.take_error() {
            return RecvInto::Failed(e);
        }
        if self.is_closed() {
            return RecvInto::Bytes(0);
        }
        RecvInto::Pending
    }

    fn drain_into(&mut self, dst: &mut [u8]) -> usize {
        let mut written = 0usize;
        while written < dst.len() {
            let Some(front) = self.recv_queue.front() else {
                break;
            };
            let slice = front.as_slice();
            let want = (dst.len() - written).min(slice.len());
            dst[written..written + want].copy_from_slice(&slice[..want]);
            written += want;
            self.recv_queued_bytes = self.recv_queued_bytes.saturating_sub(want);
            if want == slice.len() {
                self.recv_queue.pop_front();
            } else {
                self.recv_queue.front_mut().unwrap().advance(want);
            }
        }
        written
    }

    pub(super) fn send_status(&mut self, inflight: bool) -> SendIdle {
        if let Some(e) = self.take_error() {
            return SendIdle::Failed(e);
        }
        if !inflight {
            return SendIdle::Idle;
        }
        if self.is_closed() {
            return SendIdle::Failed(io::Error::new(io::ErrorKind::BrokenPipe, "fiber: closed"));
        }
        SendIdle::Pending
    }
}
