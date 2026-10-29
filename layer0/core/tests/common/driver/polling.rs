use std::{
    task::Poll,
    time::{Duration, Instant},
};

pub trait Source {
    type Output;
    type Error;

    fn try_poll(&mut self, left: Duration) -> Result<Poll<Self::Output>, Self::Error>;
}

#[derive(Debug)]
pub enum Error<E> {
    Timeout(Duration),
    Overflow(Duration),
    Source(E),
}

#[derive(Debug)]
pub struct Deadline(Duration);

impl Deadline {
    pub const fn new(timeout: Duration) -> Self {
        Self(timeout)
    }

    pub fn try_run<S: Source>(self, mut source: S) -> Result<S::Output, Error<S::Error>> {
        let deadline = Instant::now()
            .checked_add(self.0)
            .ok_or(Error::Overflow(self.0))?;
        loop {
            let left = deadline
                .checked_duration_since(Instant::now())
                .ok_or(Error::Timeout(self.0))?;
            if left.is_zero() {
                return Err(Error::Timeout(self.0));
            }
            if let Poll::Ready(output) = source.try_poll(left).map_err(Error::Source)? {
                return Ok(output);
            }
        }
    }
}
