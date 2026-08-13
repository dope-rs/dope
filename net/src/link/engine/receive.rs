use dope_core::driver::flight;

enum Arm {
    Disarmed,
    Armed { flow: Flow },
    Exhausted { paused: bool },
}

#[derive(Clone, Copy)]
enum Flow {
    Active,
    PausedPending,
    PausedInflight,
    PausedTerminal,
    ResumedInflight,
}

pub(in crate::link) struct Receive {
    arm: Arm,
}

impl Receive {
    pub(super) fn new() -> Self {
        Self { arm: Arm::Disarmed }
    }

    pub(in crate::link) fn armed<'d>(
        &mut self,
        flights: &mut super::Flights<'d>,
        flight: Option<flight::Flight<'d>>,
    ) {
        self.arm = match flight {
            Some(_) => Arm::Armed { flow: Flow::Active },
            None => Arm::Exhausted { paused: false },
        };
        flights.recv = flight;
    }

    pub(in crate::link) fn is_armed(&self) -> bool {
        matches!(self.arm, Arm::Armed { .. })
    }

    pub(in crate::link) fn needs_arm(&self, closing: bool) -> bool {
        matches!(self.arm, Arm::Exhausted { paused: false }) && !closing
    }

    pub(in crate::link) fn is_paused(&self) -> bool {
        matches!(
            self.arm,
            Arm::Armed {
                flow: Flow::PausedPending | Flow::PausedInflight,
                ..
            } | Arm::Armed {
                flow: Flow::PausedTerminal,
                ..
            } | Arm::Exhausted { paused: true }
        )
    }

    pub(in crate::link) fn block(&mut self, more: bool) {
        match &mut self.arm {
            Arm::Armed { flow, .. } => match flow {
                Flow::Active if more => *flow = Flow::PausedPending,
                Flow::Active => *flow = Flow::PausedTerminal,
                Flow::ResumedInflight => *flow = Flow::PausedInflight,
                Flow::PausedPending | Flow::PausedInflight | Flow::PausedTerminal => {}
            },
            Arm::Exhausted { paused } => *paused = true,
            Arm::Disarmed => {}
        }
    }

    pub(in crate::link) fn pause(&mut self) {
        match &mut self.arm {
            Arm::Armed { flow, .. } => match flow {
                Flow::Active => *flow = Flow::PausedPending,
                Flow::ResumedInflight => *flow = Flow::PausedInflight,
                Flow::PausedPending | Flow::PausedInflight | Flow::PausedTerminal => {}
            },
            Arm::Exhausted { paused } => *paused = true,
            Arm::Disarmed => {}
        }
    }

    pub(in crate::link) fn needs_cancel(&self) -> bool {
        matches!(
            self.arm,
            Arm::Armed {
                flow: Flow::PausedPending,
                ..
            }
        )
    }

    pub(in crate::link) fn has_inflight(&self) -> bool {
        matches!(
            self.arm,
            Arm::Armed {
                flow: Flow::Active
                    | Flow::PausedPending
                    | Flow::PausedInflight
                    | Flow::ResumedInflight,
                ..
            }
        )
    }

    pub(in crate::link) fn cancel_submitted(&mut self) {
        let Arm::Armed { flow, .. } = &mut self.arm else {
            return;
        };
        if matches!(flow, Flow::PausedPending) {
            *flow = Flow::PausedInflight;
        }
    }

    pub(in crate::link) fn resume(&mut self, closing: bool) -> bool {
        match &mut self.arm {
            Arm::Armed { flow, .. } => match flow {
                Flow::PausedPending => *flow = Flow::Active,
                Flow::PausedInflight => *flow = Flow::ResumedInflight,
                Flow::PausedTerminal => *flow = Flow::Active,
                Flow::Active | Flow::ResumedInflight => return false,
            },
            Arm::Exhausted { paused } if *paused => *paused = false,
            Arm::Disarmed | Arm::Exhausted { .. } => return false,
        }
        self.needs_arm(closing)
    }

    pub(in crate::link) fn settle(
        &mut self,
        flights: &mut super::Flights<'_>,
        more: bool,
        closing: bool,
    ) -> bool {
        if more {
            return false;
        }
        if let Some(flight) = flights.recv.take() {
            let _ = flight.complete();
        }
        let paused = self.is_paused();
        self.arm = Arm::Exhausted { paused };
        self.needs_arm(closing)
    }

    pub(in crate::link) fn cancel_kind(&self) -> u8 {
        use dope_core::driver::route::kind::RECV;
        RECV
    }

    pub(in crate::link) fn cancel_flight<'a, 'd>(
        &mut self,
        flights: &'a mut super::Flights<'d>,
    ) -> Option<&'a mut flight::Flight<'d>> {
        flights.recv.as_mut()
    }
}
