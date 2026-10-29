use o3::{alloc, depot::capacity};

/// Exact grouped-task construction failure.
#[derive(Debug)]
pub enum Error {
    Capacity(capacity::Error),
    Tasks(alloc::Error),
    Orders(alloc::Error),
    Ready(alloc::Error),
}
