use std::time;

/// Application-owned monotonic work that must bound the executor's next park.
pub trait Schedule {
    fn deadline(&self) -> Option<time::Instant>;
}
