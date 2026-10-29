pub mod canary;
mod pool;
pub mod provided;
pub mod raw;
mod source;

pub use pool::Pool;
pub use source::Source;
mod index;
pub use index::Index;

/// Exact native selection failure retains the caller's admitted buffer layout.
pub trait Selection<L: Copy + Eq> {
    type Error: Copy + Eq;
}

/// Consumes receive ownership through its native return mechanism.
///
/// Self-contained pool leases return immediately. Registered ring slots retain
/// their exact token until the owning engine publishes the deferred return.
pub trait Recycle: Sized {
    type Deferred;

    fn recycle(self) -> Option<Self::Deferred>;
}
