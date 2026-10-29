mod changes;
mod filter;
mod id;
mod index;
mod interest;
pub mod raw;
mod write;
pub use changes::Changes;
pub use filter::Filter;
pub use id::Id;
pub use index::Index;
pub use interest::Interest;
pub use write::Write;

/// Owns a fully validated deletion plan and its exclusive changelist borrow.
pub trait Delete {
    fn commit(self);
}

#[must_use = "a successful readiness submission must commit its changelist"]
pub trait Tail: raw::Batch {
    fn is_empty(&self) -> bool;
    fn len(&self) -> usize;
    fn as_mut_slice(&mut self) -> &mut [Self::Event];
    fn commit(self);
    fn retry(self);
}
