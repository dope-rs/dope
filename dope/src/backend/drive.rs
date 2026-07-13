use std::io;
use std::time::Duration;

use crate::backend::PushError;
use crate::backend::cqe::Cqe;

pub trait Drive: Sized {
    type Sqe;

    fn push(&self, sqe: Self::Sqe) -> Result<(), PushError>;

    fn submit_to_drain(&self) -> bool;

    fn drain(&self, buf: &mut [Cqe]) -> usize;

    fn park(&self, timeout: Duration) -> io::Result<()>;
}
