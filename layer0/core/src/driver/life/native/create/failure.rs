use dope_sys::{
    io::{reactor::complete, socket::addr},
    kernel, memory, probe,
    probe::{accept, receive, session},
    ring::{create, inspect, map, prepare::Rejection},
};

use crate::{
    Engine,
    driver::{
        life::native::{
            create::{Kernel, Mapping},
            host::{Receive, Selection},
        },
        result,
        result::{IoOperation, poll::fault},
        settings,
    },
    io::{socket, transfer},
};

pub(crate) use crate::driver::storage::Failure;

impl<E> From<probe::Syscall<result::Probe, E>> for Failure
where
    Kernel: From<E>,
{
    fn from(cause: probe::Syscall<result::Probe, E>) -> Self {
        Self::Kernel {
            operation: IoOperation::Probe(cause.probe),
            cause: Kernel::from(cause.error),
        }
    }
}

impl From<probe::Syscall<result::Probe, probe::Creation>> for Failure {
    fn from(cause: probe::Syscall<result::Probe, probe::Creation>) -> Self {
        match cause.error {
            probe::Creation::Syscall(error)
            | probe::Creation::Local(addr::Local::Syscall(error)) => probe::Syscall {
                probe: cause.probe,
                error,
            }
            .into(),
            probe::Creation::Local(addr::Local::Length { actual, capacity }) => {
                Self::Probe(result::ProbeError::new(
                    cause.probe,
                    result::Violation::InvalidAddress(socket::Error::Storage { actual, capacity }),
                ))
            }
        }
    }
}

impl From<accept::Check<result::Probe>> for Failure {
    fn from(check: accept::Check<result::Probe>) -> Self {
        let (probe, cause) = match check {
            accept::Check::Early(probe) => (probe, result::Violation::EarlyTermination),
            accept::Check::Outside { probe, cause } => {
                (probe, result::Violation::EscapedSlot(cause.into()))
            }
        };
        Self::Probe(result::ProbeError::new(probe, cause))
    }
}

impl From<memory::Error> for Failure {
    fn from(error: memory::Error) -> Self {
        Self::Mapping(Mapping::from(error))
    }
}

impl<R, U> From<session::Primary<R, U>> for Failure
where
    Kernel: From<R> + From<U>,
{
    fn from(primary: session::Primary<R, U>) -> Self {
        let (op, cause) = match primary {
            session::Primary::Register(error) => {
                (IoOperation::RegisterCanaryBuffers, Kernel::from(error))
            }
            session::Primary::Unregister(error) => {
                (IoOperation::UnregisterCanaryBuffers, Kernel::from(error))
            }
        };
        Self::Kernel {
            operation: op,
            cause,
        }
    }
}

impl<E> From<receive::Install<E>> for Failure
where
    Kernel: From<E>,
{
    fn from(failure: receive::Install<E>) -> Self {
        let op = match failure.mode {
            receive::Mode::Data => IoOperation::InstallReceiveFile,
            receive::Mode::Message => IoOperation::InstallReceiveMessageFile,
        };
        Self::Kernel {
            operation: op,
            cause: Kernel::from(failure.error),
        }
    }
}

impl<O, E, I, L, C> From<session::Checked<result::Probe, O, E, I, L, C, result::ProbeError>>
    for Failure
where
    Engine: complete::Observe<Observation = O>,
    Kernel: From<E>,
    Selection: From<(I, settings::Buffers)>,
    transfer::Len: From<L>,
    transfer::NonZero: From<C>,
{
    fn from(cause: session::Checked<result::Probe, O, E, I, L, C, result::ProbeError>) -> Self {
        use dope_sys::memory::buffer::canary;
        use session::Checked;

        let (probe, fault) = match cause {
            Checked::Wait(cause) => return cause.into(),
            Checked::MissingBuffer(probe) => (probe, result::Violation::MissingBuffer),
            Checked::Terminated(probe) => (probe, result::Violation::EarlyTermination),
            Checked::Buffer { probe, failure } => {
                let fault = match failure {
                    canary::Failure::Inspect(error) => {
                        return Self::Probe(error);
                    }
                    canary::Failure::Index(index) => {
                        let layout = <Receive as memory::raw::Canary>::LAYOUT;
                        result::Violation::InvalidBuffer(
                            Selection::from((index, layout.0.buffers())).into_public(),
                        )
                    }
                    canary::Failure::Length { actual, capacity } => {
                        result::Violation::BufferLength {
                            actual: actual.into(),
                            capacity: capacity.into(),
                        }
                    }
                };
                (probe, fault)
            }
        };
        Self::Probe(result::ProbeError::new(probe, fault))
    }
}

impl<C, E> From<inspect::Unsupported<C, E>> for Failure
where
    C: Into<<crate::Backend as kernel::Source>::Code>,
    Kernel: From<E>,
{
    fn from(error: inspect::Unsupported<C, E>) -> Self {
        match error {
            inspect::Unsupported::Opcode(code) => {
                Self::Capability(result::Capability::Opcode(result::Opcode::native(code)))
            }
            inspect::Unsupported::Register(error) => Self::Kernel {
                operation: IoOperation::RegisterOpcodes,
                cause: Kernel::from(error),
            },
        }
    }
}

impl From<(result::Probe, receive::Send)> for Failure {
    fn from((probe, error): (result::Probe, receive::Send)) -> Self {
        match error {
            receive::Send::Syscall(error) => Self::Kernel {
                operation: IoOperation::Probe(probe),
                cause: Kernel::from(error),
            },
            receive::Send::Partial(partial) => Self::Probe(result::ProbeError::new(
                probe,
                result::Violation::Send(fault::Send::from_partial(partial)),
            )),
        }
    }
}

impl<C, F, R> From<Rejection<C, F, R>> for Failure
where
    Kernel: From<C> + From<F> + From<R>,
{
    fn from(cause: Rejection<C, F, R>) -> Self {
        let (op, cause) = match cause {
            Rejection::Feature(feature) => {
                return Self::Capability(result::Capability::Feature(feature));
            }
            Rejection::Cancel(cause) => (IoOperation::CancelRegistrations, Kernel::from(cause)),
            Rejection::Files(cause) => (IoOperation::RegisterFileTable, Kernel::from(cause)),
            Rejection::Range(cause) => (IoOperation::RegisterFileRange, Kernel::from(cause)),
        };
        Self::Kernel {
            operation: op,
            cause,
        }
    }
}

impl<S, M> From<create::Cause<S, M>> for Failure
where
    Kernel: From<S>,
    Mapping: From<map::Map<M>>,
{
    fn from(cause: create::Cause<S, M>) -> Self {
        match cause {
            create::Cause::Setup(cause) => Self::Kernel {
                operation: IoOperation::ReactorCreate,
                cause: Kernel::from(cause),
            },
            create::Cause::Layout(cause) => Self::RingLayout(cause),
            create::Cause::Map(cause) => Self::Mapping(Mapping::from(cause)),
        }
    }
}

impl<M, R, B> From<create::Primary<M, R, B>> for Failure
where
    Self: From<M>,
    Kernel: From<R> + From<B>,
{
    fn from(primary: create::Primary<M, R, B>) -> Self {
        match primary {
            create::Primary::Ring(error) => Self::from(error),
            create::Primary::Memory(error) => {
                Self::Mapping(<Mapping as From<memory::Error>>::from(error))
            }
            create::Primary::Register(cause) => Self::Kernel {
                operation: IoOperation::RegisterReactor,
                cause: Kernel::from(cause),
            },
            create::Primary::Provided(cause) => Self::Kernel {
                operation: IoOperation::RegisterProvidedBuffers,
                cause: Kernel::from(cause),
            },
        }
    }
}

impl<S, W> From<create::Failure<S, W>> for Failure
where
    Mapping: From<S>,
    Failure: From<W>,
{
    fn from(failure: create::Failure<S, W>) -> Self {
        match failure {
            create::Failure::Storage(error) => Self::Mapping(Mapping::from(error)),
            create::Failure::Memory(error) => {
                Self::Mapping(<Mapping as From<memory::Error>>::from(error))
            }
            create::Failure::Wait(cause) => Self::from(cause),
        }
    }
}

impl<O, E> From<probe::Cause<result::Probe, O, E>> for Failure
where
    Engine: complete::Observe<Observation = O>,
    Kernel: From<E>,
{
    fn from(cause: probe::Cause<result::Probe, O, E>) -> Self {
        use probe::Cause;

        let (probe, fault) = match cause {
            Cause::Enter { probe, error } => {
                return Self::Kernel {
                    operation: IoOperation::Probe(probe),
                    cause: Kernel::from(error),
                };
            }
            Cause::Check(error) => {
                return Self::Probe(result::ProbeError::decode(error));
            }
            Cause::Missing(probe) => (probe, result::Violation::MissingCompletion),
            Cause::Unexpected { probe, observed } => (
                probe,
                result::Violation::UnexpectedCompletion {
                    expected: probe,
                    observed: fault::Observation::new(observed),
                },
            ),
            Cause::Residual {
                probe,
                submit,
                complete,
            } => (
                probe,
                result::Violation::ResidualWork {
                    submissions: fault::Pending::from(submit),
                    completions: fault::Available::from(complete),
                },
            ),
        };
        Self::Probe(result::ProbeError::new(probe, fault))
    }
}
