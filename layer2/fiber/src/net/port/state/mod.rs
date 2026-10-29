use std::{cell, mem};

use dope::net::link::event;
use o3::base::cell::region;

use crate::{
    core::{
        driver::{route::indices, schedule::ready::wake::store},
        io::transfer,
    },
    net::{
        error::{self, read},
        port::{self, entry, result},
    },
};

pub(super) struct State<'d, E> {
    terminal: TerminalCell<E>,
    pub(super) recv: store::Slot<'d>,
    pub(super) send: store::Slot<'d>,
    detached: cell::Cell<bool>,
}

mod terminal;

pub(super) use terminal::{Change, Phase, Terminal, TerminalCell};

pub(super) enum SendError {
    Closed,
    Failed,
}

pub(crate) enum Push<R> {
    Added,
    Closed(R),
    Failed(R),
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum Detach {
    First,
    Repeated,
}

impl<E> Default for State<'_, E> {
    fn default() -> Self {
        use std::cell::Cell;

        Self {
            terminal: TerminalCell::open(),
            recv: store::Slot::empty(),
            send: store::Slot::empty(),
            detached: Cell::new(false),
        }
    }
}

impl<'d, E> State<'d, E> {
    pub(crate) fn activate(&self) -> Change {
        if self.terminal.activate() == Change::Retained {
            return Change::Retained;
        }
        self.recv.clear();
        self.send.clear();
        self.detached.set(false);
        Change::Applied
    }

    pub(crate) fn push<R: transfer::Length + Unpin>(
        &self,
        lane: indices::SlotIndex,
        queue: &port::Queue<'d, R>,
        chunk: R,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Push<R> {
        let Some(input) = port::Chunk::new(chunk) else {
            return Push::Added;
        };
        if self.detached.get() {
            return Push::Closed(input.into_value());
        }
        let terminal = self.terminal.0.replace(Terminal::Closed);
        match terminal {
            Terminal::Open => {
                if let Err((chunk, cause)) = queue.push(lane, input, region) {
                    self.terminal.0.set(Terminal::LocalFailed(cause));
                    self.recv.wake_once();
                    self.send.wake_once();
                    return Push::Failed(chunk);
                }
                self.terminal.0.set(Terminal::Open);
                self.recv.wake_once();
                Push::Added
            }
            terminal => {
                self.terminal.0.set(terminal);
                Push::Closed(input.into_value())
            }
        }
    }

    pub(crate) fn eof(&self) {
        if self.terminal.eof() == Change::Applied {
            self.recv.wake_once();
        }
    }

    /// The attached I/O owner retains its write half after input ends.
    pub(crate) fn is_held(&self) -> bool {
        self.terminal.phase() == Phase::ReadClosed && !self.detached.get()
    }

    pub(crate) fn close(&self) {
        self.terminal.close();
        self.recv.wake_once();
        self.send.wake_once();
    }

    /// Releases the half-close gate while final transport retirement owns the outcome.
    pub(crate) fn begin_close(&self) {
        self.terminal.begin_close();
        self.send.wake_once();
    }

    pub(crate) fn fail_receive(&self, error: event::ParkError) {
        if self.detached.get() {
            return;
        }
        if self.terminal.fail_local(error.into()) == Change::Retained {
            return;
        }
        self.recv.wake_once();
        self.send.wake_once();
    }

    pub(crate) fn fail_edge(&self, error: read::Edge<E>) {
        if self.terminal.fail_edge(error, self.detached.get()) == Change::Retained {
            return;
        }
        self.recv.wake_once();
        self.send.wake_once();
    }

    pub(crate) fn detach(&self) -> Detach {
        if !self.detached.replace(true) {
            self.terminal.retire();
            Detach::First
        } else {
            Detach::Repeated
        }
    }

    pub(crate) fn try_recv<R: Unpin>(
        &self,
        lane: indices::SlotIndex,
        queue: &port::Queue<'d, R>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> (result::Recv<R, E>, bool) {
        use crate::net::port::result::Recv;
        let (chunk, empty) = queue.pop_state(lane, region);
        if let Some(chunk) = chunk {
            return (Recv::Ready(chunk), empty);
        }
        let result = match self.terminal.receive() {
            Terminal::Open | Terminal::Closing => Recv::Pending,
            Terminal::ReadClosed | Terminal::Closed => Recv::Closed,
            Terminal::LocalFailed(error) => Recv::Failed(error.into()),
            Terminal::EdgeFailed(error) => Recv::Failed(error.into()),
            Terminal::LocalThenEdge { local, edge } => {
                Recv::Failed(read::Read::LocalThenEdge { local, edge })
            }
        };
        (result, empty)
    }

    pub(crate) fn send_status(&self, pending: entry::Presence) -> result::SendStatus<'d> {
        use crate::net::port::result::SendStatus;
        let terminal = self.terminal.phase();
        if terminal == Phase::Failed {
            return SendStatus::Failed(error::Write::ConnectionFailed { payload: None });
        }
        match pending {
            entry::Presence::Absent => SendStatus::Complete,
            entry::Presence::Present if terminal == Phase::Closed => {
                SendStatus::Failed(error::Write::ConnectionClosed { payload: None })
            }
            entry::Presence::Present => SendStatus::Pending,
        }
    }

    pub(crate) fn start_send(&self) -> Result<(), SendError> {
        match self.terminal.phase() {
            Phase::Open | Phase::ReadClosed => Ok(()),
            Phase::Closed => Err(SendError::Closed),
            Phase::Failed => Err(SendError::Failed),
        }
    }
}

const _: () = assert!(mem::size_of::<Phase>() == 1);
