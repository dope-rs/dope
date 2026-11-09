use dope_core::driver::route::{indices, table};

use crate::network::listener::runtime;

enum Sweep {
    Open,
    Active(usize),
    Done,
}

impl Sweep {
    fn start(&mut self) {
        if matches!(self, Self::Open) {
            *self = Self::Active(0);
        }
    }

    fn take(&mut self, cap: table::Capacity) -> Option<indices::SlotIndex> {
        let Self::Active(index) = self else {
            return None;
        };
        let next = cap.into_iter().nth(*index);
        if next.is_some() {
            *index += 1;
        } else {
            *self = Self::Done;
        }
        next
    }
}

pub(in crate::network::listener) struct Schedule {
    shutdown: Sweep,
    admission: Sweep,
    pub(in crate::network::listener) phase: runtime::Phase,
}

impl Schedule {
    pub(in crate::network::listener) const fn new() -> Self {
        Self {
            shutdown: Sweep::Open,
            admission: Sweep::Open,
            phase: runtime::Phase::Accept,
        }
    }

    pub(in crate::network::listener) fn shutdown(&mut self) {
        self.shutdown.start();
    }

    pub(in crate::network::listener) fn seal(&mut self) {
        self.admission.start();
    }

    pub(in crate::network::listener) fn take_seal(
        &mut self,
        cap: table::Capacity,
    ) -> Option<indices::SlotIndex> {
        self.admission.take(cap)
    }

    pub(in crate::network::listener) fn is_open(&self) -> bool {
        matches!(self.admission, Sweep::Open)
    }

    pub(in crate::network::listener) fn is_sealing(&self) -> bool {
        matches!(self.admission, Sweep::Active(_))
    }

    pub(in crate::network::listener) fn take_close(
        &mut self,
        cap: table::Capacity,
    ) -> Option<indices::SlotIndex> {
        self.shutdown.take(cap)
    }

    pub(in crate::network::listener) fn is_closing(&self) -> bool {
        matches!(self.shutdown, Sweep::Active(_))
    }

    pub(in crate::network::listener) fn is_done(&self) -> bool {
        matches!(self.shutdown, Sweep::Done)
    }

    pub(in crate::network::listener) fn next_phase(&mut self) -> runtime::Phase {
        let phase = self.phase;
        self.phase = phase.next();
        phase
    }
}
