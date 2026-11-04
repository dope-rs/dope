use domain::retire;
use dope_sys::{
    io::{
        reactor::complete::{
            Outcome as _, Packet as _, Record as _, packet,
            packet::record::{Map as _, Selection},
            resolve::Reclaimed,
        },
        socket::option::tuning,
    },
    probe::Stopped,
    table::{
        token,
        token::{Codec as _, frame, frame::Decode as _, host, host::Drain as _},
    },
};
use poll::{
    fault,
    fault::{Raw, Violation},
};

use crate::{
    Errno,
    driver::{receive::tokens, result::poll, retained::flight, route, route::indices},
    io::{
        self, Event,
        event::batch::Flow,
        fd::{domain, handles},
        outputs, transfer,
    },
    ring::cqe::{
        Binding, CloseToken, Decoded, Frame, Observation, Packet, Record, Returned, Selected,
        Token, Tune, Tuned, Tuning, binding::Retired,
    },
};

type UserData<'q, 'd> = host::Bound<
    retire::Echo<'d>,
    handles::Retirement,
    Frame,
    flight::Complete<'q, 'd>,
    flight::raw::Echo,
    Tune,
>;

#[repr(transparent)]
pub(crate) struct Envelope<'d>(pub(super) Packet<'d>);

#[repr(transparent)]
pub(crate) struct Rejected<'d>(Stopped<Option<tokens::Token<'d, Selected>>, fault::Fault>);

impl<'d> From<Rejected<'d>> for Stopped<Option<tokens::Token<'d, Selected>>, fault::Fault> {
    fn from(rejected: Rejected<'d>) -> Self {
        rejected.0
    }
}

impl<'d> Envelope<'d> {
    pub(super) fn reject(self, subject: fault::Subject, cause: Violation) -> Rejected<'d> {
        let (parts, buffer) = self.0.into_parts();
        let observed = Observation::new(parts.token, parts.outcome, parts.flags);
        Rejected(Stopped {
            state: buffer,
            cause: fault::Fault {
                observation: fault::Observation::new(observed),
                subject,
                violation: cause,
            },
        })
    }

    fn reject_close(self, echo: retire::Echo<'_>, cause: Violation) -> Rejected<'d> {
        let _ = self.0.try_retire(echo);
        self.reject(fault::Subject::Unbound, cause)
    }

    fn reject_both(
        self,
        echo: retire::Echo<'_>,
        cause: Raw,
        expected: handles::Retirement,
        observed: handles::Retirement,
    ) -> Rejected<'d> {
        use host::Echo as _;
        echo.retry();
        self.reject(
            fault::Subject::Unbound,
            Violation::RawClass {
                cause,
                expected,
                observed,
            },
        )
    }

    fn reject_raw(self, drain: &flight::Drain<'_, 'd>, cause: Raw) -> Rejected<'d> {
        let retire = drain.driver().descriptors().retirement();
        match self
            .0
            .parts()
            .token
            .closed()
            .map_or(retire::Bind::Missing, |returned| retire.bind(returned))
        {
            retire::Bind::Ready(echo) => self.reject_close(echo, Violation::Raw(cause)),
            retire::Bind::Class {
                echo,
                expected,
                observed,
            } => self.reject_both(echo, cause, expected, observed),
            retire::Bind::Missing => self.reject(fault::Subject::Unbound, Violation::Raw(cause)),
        }
    }
}

impl<'d> From<Stopped<(Envelope<'d>, fault::Subject), Returned>> for Rejected<'d> {
    fn from(stopped: Stopped<(Envelope<'d>, fault::Subject), Returned>) -> Self {
        let Stopped {
            state: (packet, subject),
            cause,
        } = stopped;
        packet.reject(subject, Violation::Outcome(cause))
    }
}

impl<'q, 'd> From<Stopped<(Envelope<'d>, flight::Complete<'q, 'd>), Returned>> for Rejected<'d> {
    fn from(stopped: Stopped<(Envelope<'d>, flight::Complete<'q, 'd>), Returned>) -> Self {
        let Stopped {
            state: (packet, bound),
            cause,
        } = stopped;
        let flow = packet.0.flow().into();
        let subject = bound.settle(flow).into();
        packet.reject(subject, Violation::Outcome(cause))
    }
}

impl<'d> Envelope<'d> {
    pub(super) fn flow(&self) -> Flow {
        self.0.flow().into()
    }

    pub(super) fn bytes(&self) -> Result<Result<transfer::Len, Errno>, Returned> {
        Ok(self
            .0
            .parts()
            .outcome
            .bytes()?
            .map(|value| transfer::Len::from_bound(value.into())))
    }

    pub(super) fn unit(&self) -> Result<Result<(), Errno>, Returned> {
        self.0.parts().outcome.unit()
    }

    fn decode<'q>(
        drain: &'q flight::Drain<'q, 'd>,
        entry: Decoded<'d>,
    ) -> Result<(Self, UserData<'q, 'd>), Rejected<'d>> {
        const {
            assert!(std::mem::size_of::<Self>() == 3 * std::mem::size_of::<usize>());
            assert!(std::mem::align_of::<Self>() == std::mem::align_of::<usize>());
            assert!(std::mem::size_of::<Rejected<'d>>() == 8 * std::mem::size_of::<usize>());
            assert!(std::mem::align_of::<Rejected<'d>>() == std::mem::align_of::<usize>());
            assert!(std::mem::size_of::<Token>() == std::mem::size_of::<u64>());
            assert!(std::mem::size_of::<Tune>() == std::mem::size_of::<indices::SlotIndex>());
            assert!(std::mem::size_of::<Frame>() == std::mem::size_of::<route::Control>());
            assert!(std::mem::size_of::<CloseToken<'static>>() == std::mem::size_of::<u64>());
        }
        let (parts, buffer) = Record::into_parts(entry);
        let buffer = match buffer {
            Selection::None => Ok(None),
            Selection::Selected(buffer) => Ok(Some(buffer)),
            Selection::Invalid(fault) => Err(fault),
        };
        let packet = match Packet::try_new(parts, buffer) {
            Ok(packet) => Envelope(packet),
            Err(Stopped {
                state: packet,
                cause,
            }) => {
                let cause = match cause {
                    packet::Invalid::Buffer(fault) => Raw::BufferIndex(fault.into_public()),
                    packet::Invalid::Flags => Raw::Flags,
                };
                return Err(Self(packet).reject_raw(drain, cause));
            }
        };
        let user = parts.token.bind(drain);
        Ok((packet, user))
    }

    pub(crate) fn resolve<'q>(
        state: &mut Tuning,
        drain: &'q flight::Drain<'q, 'd>,
        entry: Decoded<'d>,
    ) -> Result<Option<Event<'d>>, Rejected<'d>> {
        use host::Echo as _;
        let (packet, user) = Self::decode(drain, entry)?;
        match user {
            host::Bound::Close(echo) => packet.close(echo).map(|()| None),
            host::Bound::CloseClass {
                echo,
                expected,
                observed,
            } => {
                echo.retry();
                Err(packet.class_fault(expected, observed))
            }
            host::Bound::Framework(control) => {
                if !packet.0.is_plain() {
                    return Err(packet.control(control));
                }
                match control.decode() {
                    frame::Control::Shutdown => {
                        packet
                            .0
                            .parts()
                            .outcome
                            .shutdown()
                            .map_err(|cause| Stopped {
                                state: (packet, fault::Subject::Control(control.control())),
                                cause,
                            })?;
                        Ok(Some(outputs::shutdown()))
                    }
                    frame::Control::Tuning(tune) => {
                        let (tuned, outcome) =
                            Tuned::bind(state, drain, tune, packet, control.control())?;
                        Ok(Some(tuned.complete(outcome)))
                    }
                    frame::Control::ClosePrep | frame::Control::Unknown => {
                        Err(packet.control(control))
                    }
                }
            }
            host::Bound::Invalid => {
                Err(packet.reject(fault::Subject::Unbound, Violation::UserData))
            }
            host::Bound::Cancel(cancel) => packet.cancel(state, cancel).map(|()| None),
            host::Bound::Flight(bound) => Binding::new(packet, bound).resolve().map(Some),
        }
    }

    pub(crate) fn reclaim<'q>(
        state: &mut Tuning,
        drain: &'q flight::Drain<'q, 'd>,
        entry: Decoded<'d>,
        owned: bool,
    ) -> Result<Retired<'d>, Rejected<'d>> {
        use host::Echo as _;
        let (packet, user) = Self::decode(drain, entry)?;
        let report = match user {
            host::Bound::Close(echo) => {
                packet.close(echo)?;
                None
            }
            host::Bound::CloseClass {
                echo,
                expected,
                observed,
            } => {
                echo.retry();
                return Err(packet.class_fault(expected, observed));
            }
            host::Bound::Framework(control) => {
                if !packet.0.is_plain() {
                    return Err(packet.control(control));
                }
                match control.decode() {
                    frame::Control::Tuning(tune) => {
                        let (tuned, outcome) =
                            Tuned::bind(state, drain, tune, packet, control.control())?;
                        Some(tuned.reclaim(outcome))
                    }
                    frame::Control::Shutdown => {
                        let outcome = packet.0.parts().outcome;
                        outcome.stopped().map_err(|cause| Stopped {
                            state: (packet, fault::Subject::Control(control.control())),
                            cause,
                        })?;
                        if owned && outcome.is_error() {
                            None
                        } else {
                            Some(io::Reclaimed::Shutdown)
                        }
                    }
                    frame::Control::ClosePrep => {
                        packet
                            .0
                            .parts()
                            .outcome
                            .aborted()
                            .map_err(|cause| Stopped {
                                state: (packet, fault::Subject::Control(control.control())),
                                cause,
                            })?;
                        None
                    }
                    frame::Control::Unknown => return Err(packet.control(control)),
                }
            }
            host::Bound::Invalid => {
                return Err(packet.reject(fault::Subject::Unbound, Violation::UserData));
            }
            host::Bound::Cancel(cancel) => {
                packet.cancel(state, cancel)?;
                None
            }
            host::Bound::Flight(bound) => return Binding::new(packet, bound).reclaim(),
        };
        Ok(Reclaimed {
            buffer: None,
            report,
            result: Ok(()),
        })
    }

    fn control(self, frame: Frame) -> Rejected<'d> {
        self.reject(fault::Subject::Control(frame.control()), Violation::Control)
    }

    fn cancel(
        self,
        state: &mut Tuning,
        cancel: host::Cancel<flight::raw::Echo, Tune>,
    ) -> Result<(), Rejected<'d>> {
        use token::Tuning as _;
        let subject = match &cancel {
            host::Cancel::Flight(echo) => fault::Subject::CancelFlight(fault::Echo::new(*echo)),
            host::Cancel::Tuning(tune) => fault::Subject::CancelTuning(tune.slot()),
        };
        if !self.0.is_plain() {
            return Err(self.reject(subject, Violation::Control));
        }
        let outcome = self.0.parts().outcome;
        if outcome.is_error()
            && let host::Cancel::Tuning(tune) = &cancel
        {
            tuning::Table::retry_cancel(state, tune.slot().raw());
        }
        outcome.canceled().map_err(|cause| Stopped {
            state: (self, subject),
            cause,
        })?;
        Ok(())
    }

    fn close(self, echo: retire::Echo<'d>) -> Result<(), Rejected<'d>> {
        if self.0.try_retire(echo).is_ok() {
            Ok(())
        } else {
            Err(self.reject(fault::Subject::Unbound, Violation::Close))
        }
    }

    fn class_fault(
        self,
        expected: handles::Retirement,
        observed: handles::Retirement,
    ) -> Rejected<'d> {
        self.reject(
            fault::Subject::Unbound,
            Violation::Class { expected, observed },
        )
    }
}
