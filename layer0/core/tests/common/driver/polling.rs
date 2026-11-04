use std::{
    task::Poll,
    time::{Duration, Instant},
};

#[derive(Debug)]
pub enum Error<E> {
    Timeout(Duration),
    Overflow(Duration),
    Source(E),
}

pub fn try_run<T, E>(
    timeout: Duration,
    mut poll: impl FnMut(Duration) -> Result<Poll<T>, E>,
) -> Result<T, Error<E>> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or(Error::Overflow(timeout))?;
    loop {
        let left = deadline
            .checked_duration_since(Instant::now())
            .ok_or(Error::Timeout(timeout))?;
        if left.is_zero() {
            return Err(Error::Timeout(timeout));
        }
        if let Poll::Ready(output) = poll(left).map_err(Error::Source)? {
            return Ok(output);
        }
    }
}
