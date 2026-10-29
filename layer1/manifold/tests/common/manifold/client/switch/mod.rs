use dope_manifold::client::{
    close::Reason,
    session::{
        codec::{self, Finish, Parse},
        response,
    },
};

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
    RepeatedParse,
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

pub fn accepted<H, E, S, T, F>(
    result: Result<H, E>,
    state: &mut S,
    admit: impl FnOnce(Result<&H, &E>) -> codec::Admission<F>,
) -> T
where
    T: response::Outcome,
    F: FnOnce(Result<H, E>, &mut S) -> T::Accepted,
{
    match admit(result.as_ref()) {
        codec::Admission::RetryInput => T::retry_input(),
        codec::Admission::Ready(emit) => T::accept(emit(result, state)),
    }
}

pub fn parsed<'id, H, E, S, T, F>(
    result: Result<Parse<'id, H>, E>,
    state: &mut S,
    admit: impl FnOnce(Result<&H, &E>) -> codec::Admission<F>,
) -> Result<Parse<'id, T>, T>
where
    T: response::Outcome,
    F: FnOnce(Result<H, E>, &mut S) -> T::Accepted,
{
    match result {
        Ok(Parse::Item { head, consumed }) => Ok(Parse::Item {
            head: accepted(Ok(head), state, admit),
            consumed,
        }),
        Ok(Parse::More(need)) => Ok(Parse::More(need)),
        Ok(Parse::Blocked) => Ok(Parse::Blocked),
        Ok(Parse::CapacityExhausted) => Ok(Parse::CapacityExhausted),
        Err(error) => Err(accepted(Err(error), state, admit)),
    }
}

pub fn finished<H, E, S, T, F>(
    result: Result<Finish<H>, E>,
    state: &mut S,
    admit: impl FnOnce(Result<&H, &E>) -> codec::Admission<F>,
) -> Result<Finish<T>, T>
where
    T: response::Outcome,
    F: FnOnce(Result<H, E>, &mut S) -> T::Accepted,
{
    match result {
        Ok(Finish::Item(head)) => Ok(Finish::Item(accepted(Ok(head), state, admit))),
        Ok(Finish::Empty) => Ok(Finish::Empty),
        Ok(Finish::CapacityExhausted) => Ok(Finish::CapacityExhausted),
        Err(error) => Err(accepted(Err(error), state, admit)),
    }
}
