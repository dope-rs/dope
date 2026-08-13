/// Largest successful byte count representable by every backend completion.
pub const MAX_BYTES: usize = i32::MAX as usize;

mod len;

pub use len::Len;
