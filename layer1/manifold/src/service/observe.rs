use crate::service::{self, discover};

/// Synchronous, bounded observation inside the producing discovery transition;
/// longer work must be handed to a fixed-capacity external consumer.
pub trait Audit<'d, I, A, D, const N: usize>
where
    D: discover::Output,
{
    fn resolved(
        &mut self,
        src: &D,
        set: &service::Snapshot<service::Endpoint<I, A>, N>,
        meta: &D::Metadata<'d>,
    );
    /// Observes loss of freshness without changing the installed snapshot.
    fn stale(&mut self, src: &D, rev: service::Revision);
    /// Observes withdrawal after the discovery source's retention policy ends.
    fn expired(&mut self, src: &D, rev: service::Revision);
    fn reconciled(&mut self, src: &D, rev: service::Revision, change: service::Change);
}

/// Final synchronous policy for discovery and reconciliation failures.
pub trait Failure<'d, I, A, D, const N: usize>
where
    D: discover::Output,
{
    fn failed(&mut self, src: &D, error: D::Error);
    fn rejected(&mut self, src: &D, error: service::Reconcile);
}
