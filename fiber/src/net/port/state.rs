use std::{cell, io, mem, process};

use dope::{core::driver::schedule::ready::completion, net::wire};
use o3::cell::region;

use crate::net::port::{
    recv::{arena, queue},
    result,
};

pub(super) struct State<'d> {
    recv: queue::RecvQueue,
    terminal: cell::Cell<Terminal>,
    pub(super) waiters: Waiters<'d>,
    detached: cell::Cell<bool>,
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
#[repr(u8)]
enum Terminal {
    #[default]
    Open,
    Closed,
    OutOfMemory,
}

const _: () = assert!(mem::size_of::<Terminal>() == 1);

pub(super) struct Waiters<'d> {
    recv: completion::Slot<'d>,
    send: completion::Slot<'d>,
}

impl Default for State<'_> {
    fn default() -> Self {
        use std::cell::Cell;

        use dope::core::driver::schedule::ready::completion::Slot;

        use crate::net::port::recv::queue::RecvQueue;
        Self {
            recv: RecvQueue::default(),
            terminal: Cell::new(Terminal::Open),
            waiters: Waiters {
                recv: Slot::empty(),
                send: Slot::empty(),
            },
            detached: Cell::new(false),
        }
    }
}

impl<'d> State<'d> {
    pub(crate) fn activate_empty(&self) {
        if !self.recv.is_empty() {
            process::abort();
        }
        self.terminal.set(Terminal::Open);
        self.waiters.clear();
        self.detached.set(false);
    }

    pub(crate) fn push_retained<R: wire::Cursor<'d> + 'd>(
        &self,
        lane: usize,
        arena: &arena::RecvArena<'d, R>,
        chunk: R,
        region: &mut region::Token<'d>,
    ) -> bool {
        let len = chunk.remaining();
        if len == 0 {
            return false;
        }
        if self.is_closed() || self.detached.get() {
            return true;
        }
        if arena.push(lane, &self.recv, chunk, region).is_err() {
            self.signal_out_of_memory();
            return true;
        }
        self.waiters.wake_recv();
        false
    }

    pub(crate) fn signal_out_of_memory(&self) {
        self.terminal.set(Terminal::OutOfMemory);
        self.waiters.wake_recv();
        self.waiters.wake_send();
    }

    pub(crate) fn signal_closed(&self) {
        if self.terminal.get() == Terminal::Open {
            self.terminal.set(Terminal::Closed);
        }
        self.waiters.wake_recv();
        self.waiters.wake_send();
    }

    fn is_closed(&self) -> bool {
        self.terminal.get() != Terminal::Open
    }

    pub(crate) fn detach(&self) -> bool {
        !self.detached.replace(true)
    }

    pub(crate) fn readable_drained(&self) -> bool {
        self.recv.is_empty()
    }

    pub(crate) fn try_recv<R: wire::Cursor<'d> + 'd>(
        &self,
        lane: usize,
        arena: &arena::RecvArena<'d, R>,
        region: &mut region::Token<'d>,
    ) -> result::Recv<R> {
        use crate::net::port::result::Recv;
        if let Some(chunk) = arena.take_front(lane, &self.recv, region) {
            return Recv::Ready(chunk);
        }
        match self.terminal.get() {
            Terminal::Open => Recv::Pending,
            Terminal::Closed => Recv::Closed,
            Terminal::OutOfMemory => Recv::Failed(io::ErrorKind::OutOfMemory),
        }
    }

    pub(super) fn queue(&self) -> &queue::RecvQueue {
        &self.recv
    }

    pub(crate) fn send_status(&self, inflight: bool) -> result::SendStatus {
        use crate::net::port::result::SendStatus;
        let terminal = self.terminal.get();
        if terminal == Terminal::OutOfMemory {
            return SendStatus::Failed(io::ErrorKind::OutOfMemory);
        }
        if !inflight {
            return SendStatus::Complete;
        }
        if terminal == Terminal::Closed {
            return SendStatus::Failed(io::ErrorKind::BrokenPipe);
        }
        SendStatus::Pending
    }

    pub(crate) fn start_send(&self) -> Result<(), io::ErrorKind> {
        match self.terminal.get() {
            Terminal::Open => Ok(()),
            Terminal::Closed => Err(io::ErrorKind::BrokenPipe),
            Terminal::OutOfMemory => Err(io::ErrorKind::OutOfMemory),
        }
    }
}

impl<'d> Waiters<'d> {
    fn wake(waiter: &completion::Slot<'d>) {
        if let Some(wake) = waiter.take() {
            wake.wake();
        }
    }

    pub(super) fn set_recv(&self, wake: completion::Waker<'d>) {
        self.recv.set(wake);
    }

    pub(super) fn clear_recv(&self) {
        self.recv.clear();
    }

    pub(super) fn set_send(&self, wake: completion::Waker<'d>) {
        self.send.set(wake);
    }

    pub(super) fn clear_send(&self) {
        self.send.clear();
    }

    fn clear(&self) {
        self.recv.clear();
        self.send.clear();
    }

    fn wake_recv(&self) {
        Self::wake(&self.recv);
    }

    pub(super) fn wake_send(&self) {
        Self::wake(&self.send);
    }
}
