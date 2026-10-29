pub mod poll;

use std::mem;

use dope_sys::{
    io::reactor::{Full, complete},
    kernel, probe,
    ring::{layout, prepare::Feature},
};
use o3::{alloc, buffer::pool, depot::arena::slab::Vacancy};

use crate::{
    Engine,
    driver::{
        receive,
        result::poll::fault,
        route::{indices, table::bounds},
        settings::errors,
        storage,
    },
    io::{socket, transfer},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IoOperation {
    Entropy,
    ReactorCreate,
    MapRing,
    RegisterReactor,
    CancelRegistrations,
    RegisterOpcodes,
    RegisterFileTable,
    RegisterFileRange,
    MapBuffers,
    UnmapRing,
    UnmapBuffers,
    PopulateBuffers,
    RegisterProvidedBuffers,
    RegisterCanaryBuffers,
    UnregisterCanaryBuffers,
    InstallReceiveFile,
    RemoveReceiveFile,
    InstallReceiveMessageFile,
    RemoveReceiveMessageFile,
    Probe(Probe),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cloexec {
    Get(crate::Errno),
    Set(crate::Errno),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Capability {
    Feature(Feature),
    Opcode(Opcode),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct Opcode(<crate::Backend as kernel::Source>::Code);

impl<C> Opcode
where
    crate::Backend: kernel::Source<Code = C>,
{
    pub(crate) fn native<T: Into<C>>(code: T) -> Self {
        Self(code.into())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Probe {
    Ring = 0,
    DirectSocket = 1,
    SocketOption = 2,
    CloseDirectSocket = 3,
    Receive = 4,
    ReceiveMessage = 5,
    DirectAccept = 6,
    CloseDirectAccept = 7,
    DirectAcceptMultishot = 8,
    CloseDirectAcceptMultishot = 9,
    RegisterAcceptListener = 10,
    ReleaseAcceptListener = 11,
}

impl From<Probe> for u64 {
    fn from(probe: Probe) -> Self {
        probe as Self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProbeError {
    probe: Probe,
    violation: Violation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Violation {
    MissingCompletion,
    UnexpectedCompletion {
        expected: Probe,
        observed: fault::Observation,
    },
    Rejected(fault::Observation),
    UnexpectedResult(fault::Observation),
    Send(fault::Send),
    InvalidFlags(fault::Observation),
    MissingBuffer,
    InvalidBuffer(receive::Selection),
    BufferLength {
        actual: transfer::Len,
        capacity: transfer::NonZero,
    },
    EarlyTermination,
    CorruptPayload,
    FrameOverflow {
        name: receive::Capacity,
        control: receive::Capacity,
    },
    FrameShort {
        actual: receive::Length,
        required: receive::Length,
    },
    MissingFrameTruncation {
        region: FrameRegion,
        reported: receive::Length,
        available: receive::Length,
    },
    TruncatedFrame,
    InvalidAddress(socket::Error),
    NonterminalCancellation(fault::Observation),
    EscapedSlot(bounds::Slot),
    ResidualWork {
        submissions: fault::Pending,
        completions: fault::Available,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FrameRegion {
    Control,
    Payload,
}

impl ProbeError {
    pub(crate) fn decode(
        failure: probe::Failure<Probe, <Engine as complete::Observe>::Observation>,
    ) -> Self {
        use dope_sys::probe::Check;
        let (probe, check) = failure.into_parts();
        let fault = match check {
            Check::InvalidFlags(observed) => {
                Violation::InvalidFlags(fault::Observation::new(observed))
            }
            Check::Rejected(observed) => Violation::Rejected(fault::Observation::new(observed)),
            Check::Unexpected(observed) => {
                Violation::UnexpectedResult(fault::Observation::new(observed))
            }
            Check::Nonterminal(observed) => {
                Violation::NonterminalCancellation(fault::Observation::new(observed))
            }
        };
        Self::new(probe, fault)
    }

    pub(crate) const fn new(probe: Probe, fault: Violation) -> Self {
        Self {
            probe,
            violation: fault,
        }
    }

    pub const fn into_parts(self) -> (Probe, Violation) {
        (self.probe, self.violation)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct SubmitError(SubmitCause);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubmitCause {
    /// Queue or flight capacity can become available after existing work retires.
    Full,
    /// Every identity in the retained flight group has been permanently consumed.
    IdentityExhausted,
}

impl SubmitError {
    pub(crate) const fn queue(full: Full) -> Self {
        let Full = full;
        Self(SubmitCause::Full)
    }

    pub fn cause(self) -> SubmitCause {
        self.0
    }
}

impl From<Vacancy> for SubmitError {
    fn from(error: Vacancy) -> Self {
        Self(match error {
            Vacancy::Full => SubmitCause::Full,
            Vacancy::IdentityExhausted => SubmitCause::IdentityExhausted,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct Kernel(KernelCause);

impl Kernel {
    /// Observes the exact operation and payload without exposing the native backend.
    pub fn cause(self) -> KernelCause {
        self.0
    }

    pub(crate) fn native<T: Into<KernelCause>>(cause: T) -> Self {
        Self(cause.into())
    }
}

const _: () = {
    assert!(
        mem::size_of::<Kernel>()
            == mem::size_of::<<crate::Backend as kernel::Source>::Failure<indices::SlotIndex>>()
    );
    assert!(
        mem::align_of::<Kernel>()
            == mem::align_of::<<crate::Backend as kernel::Source>::Failure<indices::SlotIndex>>()
    );
};

pub enum CreateError {
    Plan(errors::Config),
    Allocation(alloc::Error),
    Queue(alloc::Error),
    Buffer(pool::CreateError),
    RingLayout(layout::Layout),
    ReactorCloexec(Cloexec),
    Kernel {
        operation: IoOperation,
        cause: Kernel,
    },
    Mapping(storage::Mapping),
    Cleanup(storage::Cleanup),
    Removal(storage::Removal),
    Release(storage::Release),
    Capability(Capability),
    Probe(ProbeError),
}

/// Exact construction diagnostics borrowed without consuming cleanup authority.
#[derive(Debug)]
pub enum CreateKind<'a> {
    Plan(&'a errors::Config),
    Allocation(&'a alloc::Error),
    Queue(&'a alloc::Error),
    Buffer(&'a pool::CreateError),
    RingLayout(&'a layout::Layout),
    ReactorCloexec(&'a Cloexec),
    Kernel {
        operation: &'a IoOperation,
        cause: &'a Kernel,
    },
    Mapping(&'a storage::Mapping),
    Cleanup {
        primary: storage::Failure,
        cleanup: Option<storage::Failure>,
    },
    Removal(&'a storage::Removal),
    Release {
        primary: storage::Removal,
        cleanup: Option<storage::Mapping>,
    },
    Capability(&'a Capability),
    Probe(&'a ProbeError),
}

impl CreateError {
    pub fn kind(&self) -> CreateKind<'_> {
        match self {
            Self::Plan(error) => CreateKind::Plan(error),
            Self::Allocation(error) => CreateKind::Allocation(error),
            Self::Queue(error) => CreateKind::Queue(error),
            Self::Buffer(error) => CreateKind::Buffer(error),
            Self::RingLayout(error) => CreateKind::RingLayout(error),
            Self::ReactorCloexec(error) => CreateKind::ReactorCloexec(error),
            Self::Kernel {
                operation: op,
                cause,
            } => CreateKind::Kernel {
                operation: op,
                cause,
            },
            Self::Mapping(error) => CreateKind::Mapping(error),
            Self::Cleanup(state) => CreateKind::Cleanup {
                primary: state.primary(),
                cleanup: state.cleanup(),
            },
            Self::Removal(error) => CreateKind::Removal(error),
            Self::Release(state) => CreateKind::Release {
                primary: state.primary(),
                cleanup: state.cleanup(),
            },
            Self::Capability(error) => CreateKind::Capability(error),
            Self::Probe(error) => CreateKind::Probe(error),
        }
    }

    pub(crate) const fn kernel(op: IoOperation, cause: Kernel) -> Self {
        Self::Kernel {
            operation: op,
            cause,
        }
    }
}

impl From<alloc::Error> for CreateError {
    fn from(cause: alloc::Error) -> Self {
        Self::Allocation(cause)
    }
}

impl From<alloc::Overflow> for CreateError {
    fn from(cause: alloc::Overflow) -> Self {
        Self::Allocation(cause.into())
    }
}

impl From<alloc::Exhausted> for CreateError {
    fn from(cause: alloc::Exhausted) -> Self {
        Self::Allocation(cause.into())
    }
}

impl From<storage::Failure> for CreateError {
    fn from(error: storage::Failure) -> Self {
        match error {
            storage::Failure::Kernel {
                operation: op,
                cause,
            } => Self::Kernel {
                operation: op,
                cause,
            },
            storage::Failure::Mapping(error) => Self::Mapping(error),
            storage::Failure::RingLayout(error) => Self::RingLayout(error),
            storage::Failure::Capability(error) => Self::Capability(error),
            storage::Failure::Probe(error) => Self::Probe(error),
        }
    }
}

/// Exact failure phase and payload from a rejected native kernel operation.
pub type KernelCause = kernel::Diagnostic<indices::SlotIndex>;
