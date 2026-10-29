use dope_core::driver::route::{indices, table};

use crate::network::listener::runtime;

enum Shutdown {
    Open,
    Closing(Cursor),
    Done,
}

enum Admission {
    Open,
    Sealing(Cursor),
    Sealed,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
struct Cursor(usize);

impl Cursor {
    fn take(&mut self, cap: table::Capacity) -> Option<indices::SlotIndex> {
        let slot = cap.into_iter().nth(self.0)?;
        self.0 += 1;
        Some(slot)
    }
}

pub(in crate::network::listener) struct Schedule {
    shutdown: Shutdown,
    admission: Admission,
    pub(in crate::network::listener) phase: runtime::Phase,
}

impl Schedule {
    pub(in crate::network::listener) const fn new() -> Self {
        Self {
            shutdown: Shutdown::Open,
            admission: Admission::Open,
            phase: runtime::Phase::Accept,
        }
    }

    pub(in crate::network::listener) fn shutdown(&mut self) {
        if matches!(self.shutdown, Shutdown::Open) {
            self.shutdown = Shutdown::Closing(Cursor(0));
        }
    }

    pub(in crate::network::listener) fn seal(&mut self) {
        if matches!(self.admission, Admission::Open) {
            self.admission = Admission::Sealing(Cursor(0));
        }
    }

    pub(in crate::network::listener) fn take_seal(
        &mut self,
        cap: table::Capacity,
    ) -> Option<indices::SlotIndex> {
        let Admission::Sealing(cursor) = &mut self.admission else {
            return None;
        };
        let next = cursor.take(cap);
        if next.is_none() {
            self.admission = Admission::Sealed;
        }
        next
    }

    pub(in crate::network::listener) fn is_open(&self) -> bool {
        matches!(self.admission, Admission::Open)
    }

    pub(in crate::network::listener) fn is_sealing(&self) -> bool {
        matches!(self.admission, Admission::Sealing(_))
    }

    pub(in crate::network::listener) fn take_close(
        &mut self,
        cap: table::Capacity,
    ) -> Option<indices::SlotIndex> {
        let Shutdown::Closing(cursor) = &mut self.shutdown else {
            return None;
        };
        let next = cursor.take(cap);
        if next.is_none() {
            self.shutdown = Shutdown::Done;
        }
        next
    }

    pub(in crate::network::listener) fn is_closing(&self) -> bool {
        matches!(self.shutdown, Shutdown::Closing(_))
    }

    pub(in crate::network::listener) fn is_done(&self) -> bool {
        matches!(self.shutdown, Shutdown::Done)
    }

    pub(in crate::network::listener) fn next_phase(&mut self) -> runtime::Phase {
        let phase = self.phase;
        self.phase = phase.next();
        phase
    }
}
