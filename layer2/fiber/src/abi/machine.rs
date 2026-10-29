use core::task::Poll;

pub trait Terminal {
    const TERMINAL: Self;
}

impl<T> Terminal for Poll<T> {
    const TERMINAL: Self = Self::Pending;
}

impl Terminal for () {
    const TERMINAL: Self = ();
}

/// One exhaustive transition that retains the live state in place.
#[derive(Debug)]
pub enum Flow<R> {
    /// Retains the live state before returning the accompanying result.
    Suspend(R),
    /// Drops the live state before returning the terminal result.
    Complete(R),
}

/// One exhaustive transition from a live machine state.
#[derive(Debug)]
pub enum Step<T, R> {
    /// Restores the live state before returning the accompanying result.
    Suspend(T, R),
    /// Returns the result and leaves the machine terminal.
    Complete(R),
}

/// Fiber state that can be retained only through its current transition.
#[derive(Debug)]
#[repr(transparent)]
pub struct Machine<T> {
    state: Option<T>,
}

impl<T> Machine<T> {
    /// Starts with one live state.
    pub const fn new(state: T) -> Self {
        Self { state: Some(state) }
    }

    /// Borrows one live transition, or returns the terminal result.
    pub fn turn<R: Terminal>(&mut self, run: impl FnOnce(&mut T) -> Flow<R>) -> R {
        let Some(state) = self.state.as_mut() else {
            return R::TERMINAL;
        };
        match run(state) {
            Flow::Suspend(result) => result,
            Flow::Complete(result) => {
                self.state = None;
                result
            }
        }
    }

    /// Runs one live transition, or returns the terminal result.
    pub fn step<R: Terminal>(&mut self, run: impl FnOnce(T) -> Step<T, R>) -> R {
        let Some(state) = self.state.take() else {
            return R::TERMINAL;
        };
        match run(state) {
            Step::Suspend(state, result) => {
                self.state = Some(state);
                result
            }
            Step::Complete(result) => result,
        }
    }

    /// Removes a live state for cancellation, or returns `None` when none remains.
    pub fn take(&mut self) -> Option<T> {
        self.state.take()
    }
}
