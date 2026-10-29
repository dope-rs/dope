use crate::Errno;

/// A native readiness event borrowed from its delivery storage.
pub trait Event {
    fn filter(&self) -> i16;
    fn error(&self) -> Option<Errno>;
    fn data(self) -> usize;
}
