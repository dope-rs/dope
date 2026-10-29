//! Consuming terminal-state transitions.

use std::{cell, mem};

use crate::net::error::read;

pub(in crate::net::port) enum Terminal<E> {
    Open,
    ReadClosed,
    Closing,
    Closed,
    LocalFailed(read::Local),
    EdgeFailed(read::Edge<E>),
    LocalThenEdge {
        local: read::Local,
        edge: read::Edge<E>,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::net::port) enum Phase {
    Open,
    ReadClosed,
    Closed,
    Failed,
}

pub(in crate::net::port) struct TerminalCell<E>(pub(super) cell::Cell<Terminal<E>>);

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::net::port) enum Change {
    Applied,
    Retained,
}

impl<E> TerminalCell<E> {
    pub(super) const fn open() -> Self {
        Self(cell::Cell::new(Terminal::Open))
    }

    pub(super) fn phase(&self) -> Phase {
        let terminal = self.0.replace(Terminal::Closed);
        let phase = match &terminal {
            Terminal::Open => Phase::Open,
            Terminal::ReadClosed => Phase::ReadClosed,
            Terminal::Closing | Terminal::Closed => Phase::Closed,
            Terminal::LocalFailed(_) | Terminal::EdgeFailed(_) | Terminal::LocalThenEdge { .. } => {
                Phase::Failed
            }
        };
        self.0.set(terminal);
        phase
    }

    pub(super) fn activate(&self) -> Change {
        let terminal = self.0.replace(Terminal::Closed);
        match terminal {
            Terminal::Open | Terminal::ReadClosed | Terminal::Closed => {
                self.0.set(Terminal::Open);
                Change::Applied
            }
            terminal @ (Terminal::Closing
            | Terminal::LocalFailed(_)
            | Terminal::EdgeFailed(_)
            | Terminal::LocalThenEdge { .. }) => {
                self.0.set(terminal);
                Change::Retained
            }
        }
    }

    pub(super) fn eof(&self) -> Change {
        let terminal = self.0.replace(Terminal::ReadClosed);
        match terminal {
            Terminal::Open => Change::Applied,
            terminal => {
                self.0.set(terminal);
                Change::Retained
            }
        }
    }

    pub(super) fn close(&self) {
        let terminal = self.0.replace(Terminal::Closed);
        if !matches!(
            terminal,
            Terminal::Open | Terminal::ReadClosed | Terminal::Closing | Terminal::Closed
        ) {
            self.0.set(terminal);
        }
    }

    pub(super) fn begin_close(&self) {
        let terminal = self.0.replace(Terminal::Closing);
        if !matches!(terminal, Terminal::Open | Terminal::ReadClosed) {
            self.0.set(terminal);
        }
    }

    pub(super) fn fail_edge(&self, error: read::Edge<E>, detached: bool) -> Change {
        let terminal = self.0.replace(Terminal::Closed);
        match terminal {
            terminal @ (Terminal::Open | Terminal::ReadClosed | Terminal::Closing) if detached => {
                self.0.set(terminal);
                Change::Applied
            }
            Terminal::Open | Terminal::ReadClosed | Terminal::Closing => {
                self.0.set(Terminal::EdgeFailed(error));
                Change::Applied
            }
            Terminal::LocalFailed(local) => {
                self.0.set(Terminal::LocalThenEdge { local, edge: error });
                Change::Applied
            }
            terminal @ (Terminal::Closed
            | Terminal::EdgeFailed(_)
            | Terminal::LocalThenEdge { .. }) => {
                self.0.set(terminal);
                Change::Retained
            }
        }
    }

    pub(super) fn fail_local(&self, error: read::Local) -> Change {
        let terminal = self.0.replace(Terminal::Closed);
        match terminal {
            Terminal::Open | Terminal::ReadClosed | Terminal::Closing => {
                self.0.set(Terminal::LocalFailed(error));
                Change::Applied
            }
            Terminal::EdgeFailed(edge) => {
                self.0.set(Terminal::LocalThenEdge { local: error, edge });
                Change::Applied
            }
            terminal @ (Terminal::LocalFailed(_)
            | Terminal::Closed
            | Terminal::LocalThenEdge { .. }) => {
                self.0.set(terminal);
                Change::Retained
            }
        }
    }

    pub(super) fn retire(&self) {
        let terminal = self.0.replace(Terminal::Closed);
        match terminal {
            Terminal::LocalFailed(_)
            | Terminal::EdgeFailed(_)
            | Terminal::LocalThenEdge { .. }
            | Terminal::Closed => {}
            terminal @ (Terminal::Open | Terminal::ReadClosed | Terminal::Closing) => {
                self.0.set(terminal);
            }
        }
    }

    pub(super) fn receive(&self) -> Terminal<E> {
        let terminal = self.0.replace(Terminal::Closed);
        self.0.set(match &terminal {
            Terminal::Open => Terminal::Open,
            Terminal::ReadClosed => Terminal::ReadClosed,
            Terminal::Closing => Terminal::Closing,
            _ => Terminal::Closed,
        });
        terminal
    }
}

const _: () = assert!(mem::size_of::<TerminalCell<()>>() == mem::size_of::<Terminal<()>>());
