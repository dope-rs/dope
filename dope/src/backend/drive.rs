use std::io;
use std::time::Duration;

use crate::backend::PushError;
use crate::backend::cqe::Cqe;

pub trait Drive: Sized + 'static {
    type Sqe;

    fn push(&mut self, sqe: Self::Sqe) -> Result<(), PushError>;

    fn drain(&mut self, buf: &mut [Cqe]) -> usize;

    fn park(&mut self, timeout: Duration) -> io::Result<()>;
}
