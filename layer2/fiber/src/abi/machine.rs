use core::{ops::ControlFlow, task::Poll};

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
    pub fn turn<O>(&mut self, run: impl FnOnce(&mut T) -> Poll<O>) -> Poll<O> {
        let Some(state) = self.state.as_mut() else {
            return Poll::Pending;
        };
        let result = run(state);
        if result.is_ready() {
            self.state = None;
        }
        result
    }

    /// Runs one live transition, or returns the terminal result.
    pub fn step<O>(&mut self, run: impl FnOnce(T) -> ControlFlow<O, T>) -> Poll<O> {
        let Some(state) = self.state.take() else {
            return Poll::Pending;
        };
        match run(state) {
            ControlFlow::Continue(state) => {
                self.state = Some(state);
                Poll::Pending
            }
            ControlFlow::Break(output) => Poll::Ready(output),
        }
    }

    /// Removes a live state for cancellation, or returns `None` when none remains.
    pub fn take(&mut self) -> Option<T> {
        self.state.take()
    }
}
