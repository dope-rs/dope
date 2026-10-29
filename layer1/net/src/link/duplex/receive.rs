use std::mem;

use dope_core::{
    driver::{retained::flight, route},
    io::event::batch::Flow,
};
use route::kind;

use crate::link::duplex::ops::Cancel;

#[repr(u16)]
#[derive(Clone, Copy)]
enum Arm {
    Disarmed,
    Active,
    ResourcePending,
    ApplicationPending,
    BothPending,
    ResourceInflight,
    ApplicationInflight,
    BothInflight,
    ResumedInflight,
    Exhausted,
    ResourceExhausted,
    ApplicationExhausted,
    BothExhausted,
    BufferProbeReady,
    BufferProbeActive,
    BufferWaiting,
    BufferReady,
    CancelRejected,
}

pub(in crate::link) enum Observation {
    Normal(Rearm),
    Probe(Rearm),
}

impl Observation {
    pub(in crate::link) const fn rearm(&self) -> Rearm {
        match self {
            Self::Normal(rearm) | Self::Probe(rearm) => *rearm,
        }
    }

    pub(in crate::link) const fn is_probe(&self) -> bool {
        match self {
            Self::Normal(Rearm::Idle | Rearm::Needed) => false,
            Self::Probe(Rearm::Idle | Rearm::Needed) => true,
        }
    }
}

#[derive(Clone, Copy)]
pub(in crate::link) enum Rearm {
    Idle,
    Needed,
}

impl Rearm {
    const fn from_needed(needed: bool) -> Self {
        if needed { Self::Needed } else { Self::Idle }
    }

    pub(in crate::link) const fn is_needed(self) -> bool {
        matches!(self, Self::Needed)
    }
}

pub(in crate::link) enum Pause {
    Active,
    Exhausted,
}

impl Pause {
    pub(in crate::link) const fn is_exhausted(&self) -> bool {
        matches!(self, Self::Exhausted)
    }
}

pub(in crate::link) enum Start {
    Started,
    Busy,
}

impl Start {
    pub(in crate::link) const fn is_started(&self) -> bool {
        matches!(self, Self::Started)
    }
}

/// Application pause persists across buffer recovery.
pub(in crate::link) struct Receive<'d>(Arm, Option<flight::Flight<'d, kind::Recv>>, bool);

pub(in crate::link) struct Exhaustion<'a>(&'a mut Arm);

pub(in crate::link) struct Resume<'a>(&'a mut Arm);

impl<'d> Receive<'d> {
    pub(super) fn new() -> Self {
        Self(Arm::Disarmed, None, false)
    }

    pub(super) fn armed(&mut self, flight: flight::Flight<'d, kind::Recv>) {
        self.1 = Some(flight);
        self.0 = match self.0 {
            Arm::BufferProbeReady => Arm::BufferProbeActive,
            _ => Arm::Active,
        };
    }

    pub(in crate::link) fn is_armed(&self) -> bool {
        matches!(
            self.0,
            Arm::Active
                | Arm::ResourcePending
                | Arm::ApplicationPending
                | Arm::BothPending
                | Arm::ResourceInflight
                | Arm::ApplicationInflight
                | Arm::BothInflight
                | Arm::ResumedInflight
                | Arm::BufferProbeActive
                | Arm::CancelRejected
        )
    }

    pub(in crate::link) fn is_armable(&self, closing: bool) -> bool {
        matches!(
            self.0,
            Arm::Exhausted | Arm::BufferProbeReady | Arm::BufferWaiting | Arm::BufferReady
        ) && !closing
            && !self.2
    }

    pub(in crate::link) fn is_paused(&self) -> bool {
        self.2
            || matches!(
                self.0,
                Arm::ResourcePending
                    | Arm::ApplicationPending
                    | Arm::BothPending
                    | Arm::ResourceInflight
                    | Arm::ApplicationInflight
                    | Arm::BothInflight
                    | Arm::ResourceExhausted
                    | Arm::ApplicationExhausted
                    | Arm::BothExhausted
                    | Arm::CancelRejected
            )
    }

    pub(in crate::link) const fn is_app_held(&self) -> bool {
        self.2
    }

    pub(in crate::link) fn pause(&mut self) {
        self.0 = match self.0 {
            Arm::Active => Arm::ResourcePending,
            Arm::ApplicationPending => Arm::BothPending,
            Arm::ApplicationInflight => Arm::BothInflight,
            Arm::ResumedInflight => Arm::ResourceInflight,
            Arm::ApplicationExhausted => Arm::BothExhausted,
            Arm::Exhausted => Arm::ResourceExhausted,
            state => state,
        };
    }

    pub(in crate::link) fn pause_app(&mut self) -> Pause {
        self.2 = true;
        let pause = match self.0 {
            Arm::Exhausted => Pause::Exhausted,
            Arm::Disarmed
            | Arm::Active
            | Arm::ResourcePending
            | Arm::ApplicationPending
            | Arm::BothPending
            | Arm::ResourceInflight
            | Arm::ApplicationInflight
            | Arm::BothInflight
            | Arm::ResumedInflight
            | Arm::ResourceExhausted
            | Arm::ApplicationExhausted
            | Arm::BothExhausted
            | Arm::BufferProbeReady
            | Arm::BufferProbeActive
            | Arm::BufferWaiting
            | Arm::BufferReady
            | Arm::CancelRejected => Pause::Active,
        };
        self.0 = match self.0 {
            Arm::Active => Arm::ApplicationPending,
            Arm::ResourcePending => Arm::BothPending,
            Arm::ResourceInflight => Arm::BothInflight,
            Arm::ResumedInflight => Arm::ApplicationInflight,
            Arm::ResourceExhausted => Arm::BothExhausted,
            Arm::Exhausted => Arm::ApplicationExhausted,
            state => state,
        };
        pause
    }

    pub(in crate::link) fn has_cancel(&self) -> bool {
        matches!(
            self.0,
            Arm::ResourcePending | Arm::ApplicationPending | Arm::BothPending
        )
    }

    pub(in crate::link) fn exhaustion(&mut self) -> Exhaustion<'_> {
        Exhaustion(&mut self.0)
    }

    pub(in crate::link) fn deny_cancel(&mut self) {
        self.0 = Arm::CancelRejected;
    }

    pub(in crate::link) fn cancel_sent(&mut self) {
        self.0 = match self.0 {
            Arm::ResourcePending => Arm::ResourceInflight,
            Arm::ApplicationPending => Arm::ApplicationInflight,
            Arm::BothPending => Arm::BothInflight,
            state => state,
        };
    }

    pub(in crate::link) fn resume(&mut self, closing: bool) -> Option<Rearm> {
        let next = match self.0 {
            Arm::ResourcePending => Arm::Active,
            Arm::BothPending => Arm::ApplicationPending,
            Arm::ResourceInflight => Arm::ResumedInflight,
            Arm::BothInflight => Arm::ApplicationInflight,
            Arm::ResourceExhausted => Arm::Exhausted,
            Arm::BothExhausted => Arm::ApplicationExhausted,
            Arm::Disarmed
            | Arm::Active
            | Arm::ApplicationPending
            | Arm::ApplicationInflight
            | Arm::ResumedInflight
            | Arm::Exhausted
            | Arm::ApplicationExhausted
            | Arm::BufferProbeReady
            | Arm::BufferProbeActive
            | Arm::BufferWaiting
            | Arm::BufferReady
            | Arm::CancelRejected => return None,
        };
        self.0 = next;
        Some(Rearm::from_needed(self.is_armable(closing)))
    }

    pub(in crate::link) fn resume_app(&mut self, closing: bool) -> Option<Rearm> {
        if !self.2 {
            return None;
        }
        self.2 = false;
        let next = match self.0 {
            Arm::ApplicationPending => Arm::Active,
            Arm::BothPending => Arm::ResourcePending,
            Arm::ApplicationInflight => Arm::ResumedInflight,
            Arm::BothInflight => Arm::ResourceInflight,
            Arm::ApplicationExhausted => Arm::Exhausted,
            Arm::BothExhausted => Arm::ResourceExhausted,
            Arm::Disarmed
            | Arm::Active
            | Arm::ResourcePending
            | Arm::ResourceInflight
            | Arm::ResumedInflight
            | Arm::Exhausted
            | Arm::ResourceExhausted
            | Arm::BufferProbeReady
            | Arm::BufferProbeActive
            | Arm::BufferWaiting
            | Arm::BufferReady
            | Arm::CancelRejected => self.0,
        };
        self.0 = next;
        Some(Rearm::from_needed(self.is_armable(closing)))
    }

    pub(super) fn observe(&mut self, flow: Flow, closing: bool) -> Option<Observation> {
        match flow {
            Flow::More => {
                return match self.0 {
                    Arm::BufferProbeActive => Some(Observation::Probe(Rearm::Idle)),
                    Arm::Active
                    | Arm::ResourcePending
                    | Arm::ApplicationPending
                    | Arm::BothPending
                    | Arm::ResourceInflight
                    | Arm::ApplicationInflight
                    | Arm::BothInflight
                    | Arm::ResumedInflight
                    | Arm::CancelRejected => Some(Observation::Normal(Rearm::Idle)),
                    Arm::Disarmed
                    | Arm::Exhausted
                    | Arm::ResourceExhausted
                    | Arm::ApplicationExhausted
                    | Arm::BothExhausted
                    | Arm::BufferProbeReady
                    | Arm::BufferWaiting
                    | Arm::BufferReady => None,
                };
            }
            Flow::Last => {}
        }
        let probe = matches!(self.0, Arm::BufferProbeActive);
        let next = match self.0 {
            Arm::Active | Arm::ResumedInflight => Arm::Exhausted,
            Arm::ResourcePending | Arm::ResourceInflight => Arm::ResourceExhausted,
            Arm::ApplicationPending | Arm::ApplicationInflight => Arm::ApplicationExhausted,
            Arm::BothPending | Arm::BothInflight => Arm::BothExhausted,
            Arm::BufferProbeActive => Arm::BufferWaiting,
            Arm::CancelRejected => Arm::Disarmed,
            Arm::Disarmed
            | Arm::Exhausted
            | Arm::ResourceExhausted
            | Arm::ApplicationExhausted
            | Arm::BothExhausted
            | Arm::BufferProbeReady
            | Arm::BufferWaiting
            | Arm::BufferReady => return None,
        };
        self.1 = None;
        self.0 = next;
        let rearm = Rearm::from_needed(self.is_armable(closing));
        Some(if probe {
            Observation::Probe(rearm)
        } else {
            Observation::Normal(rearm)
        })
    }

    pub(super) fn cancel(&mut self) -> Cancel<'_, 'd, kind::Recv> {
        match self.1.as_mut() {
            None => Cancel::Idle,
            Some(flight) => Cancel::Armed(flight),
        }
    }
}

impl Exhaustion<'_> {
    pub(in crate::link) fn try_probe(&mut self) -> Start {
        if !matches!(*self.0, Arm::Exhausted) {
            return Start::Busy;
        }
        *self.0 = Arm::BufferProbeReady;
        Start::Started
    }

    pub(in crate::link) fn pending(&mut self) {
        *self.0 = Arm::BufferWaiting;
    }

    pub(in crate::link) fn has_waiter(&self) -> bool {
        matches!(
            *self.0,
            Arm::BufferProbeReady | Arm::BufferProbeActive | Arm::BufferWaiting | Arm::BufferReady
        )
    }

    pub(in crate::link) fn bind(&mut self) -> Option<Resume<'_>> {
        matches!(*self.0, Arm::BufferWaiting | Arm::BufferReady).then_some(Resume(self.0))
    }

    pub(in crate::link) fn try_resume(&mut self) -> Start {
        if !matches!(*self.0, Arm::BufferWaiting) {
            return Start::Busy;
        }
        *self.0 = Arm::BufferReady;
        Start::Started
    }

    pub(in crate::link) fn settle_probe(&mut self) {
        *self.0 = match *self.0 {
            Arm::BufferProbeActive => Arm::Active,
            Arm::BufferWaiting => Arm::Exhausted,
            state => state,
        };
    }

    pub(in crate::link) fn cancel(&mut self) {
        *self.0 = match *self.0 {
            Arm::BufferProbeActive => Arm::Active,
            Arm::BufferProbeReady | Arm::BufferWaiting | Arm::BufferReady => Arm::Exhausted,
            state => state,
        };
    }
}

impl Resume<'_> {
    pub(in crate::link) fn commit(self) {
        *self.0 = Arm::BufferReady;
    }

    pub(in crate::link) fn wait(self) {
        *self.0 = Arm::BufferWaiting;
    }

    pub(in crate::link) fn cancel(self) {
        *self.0 = Arm::Exhausted;
    }
}

const _: () = {
    let fields = mem::size_of::<Arm>() + mem::size_of::<Option<flight::Flight<'_, kind::Recv>>>();
    let align = mem::align_of::<Receive<'_>>();
    assert!(mem::size_of::<Receive<'_>>() == fields.div_ceil(align) * align);
    assert!(mem::size_of::<Exhaustion<'_>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Resume<'_>>() == mem::size_of::<usize>());
};
