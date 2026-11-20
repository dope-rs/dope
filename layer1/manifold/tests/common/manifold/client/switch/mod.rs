use dope_manifold::client::close::Reason;

pub mod barrier;
pub mod decode;
pub mod eof;
pub mod retry;
pub mod terminal;

#[derive(Debug)]
pub enum Error {
    Phase {
        actual: u8,
        expected: u8,
    },
    Unregistered,
    PendingOutput,
    AttemptMismatch,
    TargetMismatch,
    EmptyDuplex,
    Suffix {
        actual: *const u8,
        expected: *const u8,
    },
    Attempts {
        actual: usize,
        limit: usize,
    },
    TerminalBytes(Vec<u8>),
    UnparsedEof,
    RepeatedReject,
    RejectedEof,
    RejectedState(bool),
    ParseCount {
        actual: usize,
        recorded: usize,
    },
    FinishCount {
        actual: usize,
        recorded: usize,
    },
    Settlement {
        closing: bool,
        quarantines: u32,
    },
    Mode {
        actual: barrier::Mode,
        framed: bool,
        duplex: bool,
        pending: bool,
    },
    Retired {
        bytes: Vec<u8>,
        settlements: usize,
    },
    Close(Reason),
}
