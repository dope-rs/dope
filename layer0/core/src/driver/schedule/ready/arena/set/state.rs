use std::cell;

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub(in crate::driver::schedule::ready) enum Side {
    A,
    B,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::driver::schedule::ready::arena::set) enum Phase {
    Idle,
    Draining,
    Paused(Side),
}

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
enum Mode {
    IdleA,
    IdleB,
    DrainingA,
    DrainingB,
    PausedA,
    PausedB,
}

pub(in crate::driver::schedule::ready::arena::set) struct State {
    mode: cell::Cell<Mode>,
}

impl Side {
    pub(in crate::driver::schedule::ready::arena::set) const fn index(self) -> usize {
        match self {
            Self::A => 0,
            Self::B => 1,
        }
    }

    pub(in crate::driver::schedule::ready::arena::set) const fn other(self) -> Self {
        match self {
            Self::A => Self::B,
            Self::B => Self::A,
        }
    }
}

impl State {
    pub(in crate::driver::schedule::ready::arena::set) const fn new() -> Self {
        Self {
            mode: cell::Cell::new(Mode::IdleA),
        }
    }

    pub(in crate::driver::schedule::ready::arena::set) fn active(&self) -> Side {
        match self.mode.get() {
            Mode::IdleA => Side::A,
            Mode::IdleB => Side::B,
            Mode::DrainingA | Mode::PausedA => Side::B,
            Mode::DrainingB | Mode::PausedB => Side::A,
        }
    }

    pub(in crate::driver::schedule::ready::arena::set) fn phase(&self) -> Phase {
        match self.mode.get() {
            Mode::IdleA | Mode::IdleB => Phase::Idle,
            Mode::DrainingA | Mode::DrainingB => Phase::Draining,
            Mode::PausedA => Phase::Paused(Side::A),
            Mode::PausedB => Phase::Paused(Side::B),
        }
    }

    pub(in crate::driver::schedule::ready::arena::set) fn idle(&self, active: Side) {
        self.mode.set(match active {
            Side::A => Mode::IdleA,
            Side::B => Mode::IdleB,
        });
    }

    pub(in crate::driver::schedule::ready::arena::set) fn draining(&self, side: Side) {
        self.mode.set(match side {
            Side::A => Mode::DrainingA,
            Side::B => Mode::DrainingB,
        });
    }

    pub(in crate::driver::schedule::ready::arena::set) fn paused(&self, side: Side) {
        self.mode.set(match side {
            Side::A => Mode::PausedA,
            Side::B => Mode::PausedB,
        });
    }
}

const _: () = assert!(std::mem::size_of::<State>() == 1);
