use domain::retire;
use dope_sys::{
    engine,
    io::{
        file,
        reactor::{
            complete,
            complete::{
                Observe, Outcome as _, Record as _, packet,
                packet::record::{Map as _, Selection},
                resolve::Reclaimed,
            },
        },
        socket::option::{tuning, tuning::Source},
    },
    kernel,
    memory::buffer::provided,
    probe::Stopped,
    ring,
    table::{
        token,
        token::{
            Codec as _, frame,
            frame::Decode as _,
            host,
            host::{Drain as _, Flight as _},
        },
    },
};
use poll::{
    fault,
    fault::{Raw, Violation},
};

use crate::{
    Errno,
    driver::{
        life::native, receive::tokens, result::poll, retained::flight, route, route::indices,
    },
    io::{
        self, Event,
        event::batch::Flow,
        fd::{domain, handles},
        outputs, transfer,
    },
    ring::cqe::{
        Binding, CloseToken, Completed, Decoded, Flags, Frame, Observation, Opened, Parts, Record,
        Returned, Scalar, Selected, Token, Tune, Tuned, Tuning, Value, binding::Retired, tuned,
    },
};

type UserData<'q, 'd, N> = host::Bound<
    retire::Echo<'d>,
    handles::Retirement,
    Frame<N>,
    flight::Complete<'q, 'd>,
    flight::raw::Echo,
    Tune<N>,
>;

#[repr(transparent)]
pub(crate) struct Envelope<P>(pub(super) P);

#[repr(transparent)]
pub(crate) struct Rejected<P: complete::Packet>(Stopped<Option<P::Buffer>, fault::Fault>);

impl<P: complete::Packet> From<Rejected<P>> for Stopped<Option<P::Buffer>, fault::Fault> {
    fn from(rejected: Rejected<P>) -> Self {
        rejected.0
    }
}

impl<'d, N, P> Envelope<P>
where
    P: ring::Packet<Native = N, Parts = Parts<N>, Buffer = tokens::Token<'d, Selected<N>>>,
    N: ring::Source + token::Source + provided::Source,
    N: Observe<Token<N>>,
    Observation<N>: complete::Record<Token<N>, Outcome = Scalar<N>, Flags = Flags<N>>
        + Into<<crate::Engine as Observe>::Observation>,
    Returned<N>: Into<<crate::Backend as kernel::Source>::Returned>,
{
    pub(super) fn reject(self, subject: fault::Subject, cause: Violation) -> Rejected<P> {
        let (parts, buffer) = self.0.into_parts();
        let observed = Observation::<N>::new(parts.token, parts.outcome, parts.flags);
        Rejected(Stopped {
            state: buffer,
            cause: fault::Fault {
                observation: fault::Observation::new(observed.into()),
                subject,
                violation: cause,
            },
        })
    }

    fn reject_close(self, echo: retire::Echo<'_>, cause: Violation) -> Rejected<P> {
        let _ = self.0.try_retire(echo);
        self.reject(fault::Subject::Unbound, cause)
    }

    fn reject_both(
        self,
        echo: retire::Echo<'_>,
        cause: Raw,
        expected: handles::Retirement,
        observed: handles::Retirement,
    ) -> Rejected<P> {
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

    fn reject_raw(self, drain: &flight::Drain<'_, 'd>, cause: Raw) -> Rejected<P> {
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

impl<'d, N, P, S, E> From<Stopped<(Envelope<P>, S), E>> for Rejected<P>
where
    P: ring::Packet<Native = N, Parts = Parts<N>, Buffer = tokens::Token<'d, Selected<N>>>,
    N: ring::Source + token::Source + provided::Source + Observe<Token<N>>,
    Observation<N>: complete::Record<Token<N>, Outcome = Scalar<N>, Flags = Flags<N>>
        + Into<<crate::Engine as Observe>::Observation>,
    Returned<N>: Into<<crate::Backend as kernel::Source>::Returned>,
    fault::Subject: From<S>,
    E: Into<<crate::Backend as kernel::Source>::Returned>,
{
    fn from(stopped: Stopped<(Envelope<P>, S), E>) -> Self {
        let Stopped {
            state: (packet, subject),
            cause,
        } = stopped;
        packet.reject(
            fault::Subject::from(subject),
            Violation::Outcome(cause.into()),
        )
    }
}

impl<'q, 'd, N, P, E> From<Stopped<(Envelope<P>, flight::Complete<'q, 'd>), E>> for Rejected<P>
where
    P: ring::Packet<Native = N, Parts = Parts<N>, Buffer = tokens::Token<'d, Selected<N>>>,
    N: ring::Source + token::Source + provided::Source + Observe<Token<N>>,
    Observation<N>: complete::Record<Token<N>, Outcome = Scalar<N>, Flags = Flags<N>>
        + Into<<crate::Engine as Observe>::Observation>,
    Returned<N>: Into<<crate::Backend as kernel::Source>::Returned>,
    E: Into<<crate::Backend as kernel::Source>::Returned>,
{
    fn from(stopped: Stopped<(Envelope<P>, flight::Complete<'q, 'd>), E>) -> Self {
        let Stopped {
            state: (packet, bound),
            cause,
        } = stopped;
        let flow = packet.0.flow().into();
        let subject = bound.settle(flow).into();
        packet.reject(subject, Violation::Outcome(cause.into()))
    }
}

impl<'d, N, P> Envelope<P>
where
    P: ring::Packet<Native = N, Parts = Parts<N>, Buffer = tokens::Token<'d, Selected<N>>>,
    N: ring::Source + token::Source + provided::Source,
    Returned<N>:
        kernel::Returned<Value = Value<N>, Invalid = <Scalar<N> as complete::Outcome>::Invalid>,
{
    pub(super) fn flow(&self) -> Flow {
        self.0.flow().into()
    }

    pub(super) fn bytes(&self) -> Result<Result<transfer::Len, Errno>, Returned<N>> {
        Ok(self
            .0
            .parts()
            .outcome
            .bytes()?
            .map(|value| transfer::Len::from_bound(value.into())))
    }

    pub(super) fn unit(&self) -> Result<Result<(), Errno>, Returned<N>> {
        self.0.parts().outcome.unit()
    }
}

impl<'d, N, P> Envelope<P>
where
    P: ring::Packet<Native = N, Parts = Parts<N>, Buffer = tokens::Token<'d, Selected<N>>>,
    N: ring::Source + token::Source + provided::Source,
    N: Observe<Token<N>>,
    Observation<N>: complete::Record<Token<N>, Outcome = Scalar<N>, Flags = Flags<N>>
        + Into<<crate::Engine as Observe>::Observation>,
    Returned<N>: Into<<crate::Backend as kernel::Source>::Returned>,
    N: Source + file::Completion,
    Opened<N>: file::raw::Completion<
            Outcome = Scalar<N>,
            Invalid = <Scalar<N> as complete::Outcome>::Invalid,
        >,
    Returned<N>:
        kernel::Returned<Value = Value<N>, Invalid = <Scalar<N> as complete::Outcome>::Invalid>,
    Completed<N>: Into<<crate::Engine as engine::Buffer>::Token>,
{
    fn try_control(
        self,
        control: route::Control,
    ) -> Result<(Self, Result<(), Errno>), Rejected<P>> {
        let outcome = self.unit();
        let ((packet, _), outcome) =
            Stopped::attach((self, fault::Subject::Control(control)), outcome)?;
        Ok((packet, outcome))
    }

    fn reject_tuned<'q, S>(self, frame: Frame<N>, cause: tuned::Failure<'q, 'd, S>) -> Rejected<P>
    where
        S: tuning::Occupied<flight::raw::Echo>,
    {
        use tuning::Missing;
        let control = frame.control();
        let (subject, cause) = match cause {
            tuned::Failure::Lookup(missing) => {
                let cause = match missing {
                    Missing::Slot => Violation::Slot,
                    Missing::Vacant => Violation::Control,
                };
                (fault::Subject::Control(control), cause)
            }
            tuned::Failure::Stale(Stopped { state, cause: echo }) => {
                state.clear();
                let echo = fault::Echo::new(echo);
                (
                    fault::Subject::Tuning { echo, control },
                    Violation::Stale(echo),
                )
            }
            tuned::Failure::Class(Stopped {
                state: (state, bound),
                cause: observed,
            }) => {
                state.clear();
                let echo = fault::Echo::new(bound.end(Flow::Last));
                (
                    fault::Subject::Tuning { echo, control },
                    Violation::Tuning(observed),
                )
            }
        };
        self.reject(subject, cause)
    }

    fn decode<'q>(
        drain: &'q flight::Drain<'q, 'd>,
        entry: Decoded<tokens::Token<'d, Selected<N>>, N>,
    ) -> Result<(Self, UserData<'q, 'd, N>), Rejected<P>> {
        const {
            assert!(std::mem::size_of::<Self>() == 3 * std::mem::size_of::<usize>());
            assert!(std::mem::align_of::<Self>() == std::mem::align_of::<usize>());
            assert!(std::mem::size_of::<Rejected<P>>() == 8 * std::mem::size_of::<usize>());
            assert!(std::mem::align_of::<Rejected<P>>() == std::mem::align_of::<usize>());
            assert!(std::mem::size_of::<Token<N>>() == std::mem::size_of::<u64>());
            assert!(std::mem::size_of::<Tune<N>>() == std::mem::size_of::<indices::SlotIndex>());
            assert!(std::mem::size_of::<Frame<N>>() == std::mem::size_of::<route::Control>());
            assert!(std::mem::size_of::<CloseToken<'static, N>>() == std::mem::size_of::<u64>());
        }
        let (parts, buffer) = Record::<N>::into_parts(entry);
        let buffer = match buffer {
            Selection::None => Ok(None),
            Selection::Selected(buffer) => Ok(Some(buffer)),
            Selection::Invalid(fault) => Err(fault),
        };
        let packet = match P::try_new(parts, buffer) {
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
        state: &mut Tuning<N>,
        drain: &'q flight::Drain<'q, 'd>,
        entry: Decoded<tokens::Token<'d, Selected<N>>, N>,
    ) -> Result<Option<Event<'d>>, Rejected<P>> {
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
                        let (packet, outcome) = packet.try_control(control.control())?;
                        let tuned = Tuned::bind(state, drain, tune)
                            .map_err(|cause| packet.reject_tuned(control, cause))?;
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
        state: &mut Tuning<N>,
        drain: &'q flight::Drain<'q, 'd>,
        entry: Decoded<tokens::Token<'d, Selected<N>>, N>,
        owned: bool,
    ) -> Result<Retired<'d, N, P>, Rejected<P>> {
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
                        let (packet, outcome) = packet.try_control(control.control())?;
                        let tuned = Tuned::bind(state, drain, tune)
                            .map_err(|cause| packet.reject_tuned(control, cause))?;
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
                            Some(native::Report(io::Reclaimed::Shutdown))
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

    fn control(self, frame: Frame<N>) -> Rejected<P> {
        self.reject(fault::Subject::Control(frame.control()), Violation::Control)
    }

    fn cancel(
        self,
        state: &mut Tuning<N>,
        cancel: host::Cancel<flight::raw::Echo, Tune<N>>,
    ) -> Result<(), Rejected<P>> {
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

    fn close(self, echo: retire::Echo<'d>) -> Result<(), Rejected<P>> {
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
    ) -> Rejected<P> {
        self.reject(
            fault::Subject::Unbound,
            Violation::Class { expected, observed },
        )
    }
}
