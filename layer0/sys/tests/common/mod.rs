mod batch;
pub mod io_contracts;
pub mod record;
pub mod thread;

pub use batch::Batch;

pub const ENTRIES: [Result<(), u16>; 3] = [Err(7), Ok(()), Err(11)];
