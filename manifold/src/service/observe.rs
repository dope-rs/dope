use crate::service;

/// Synchronous, bounded observation inside the producing discovery transition;
/// longer work must be handed to a fixed-capacity external consumer.
pub trait Observe<I, A, M, E, const N: usize> {
    fn resolved(&mut self, snapshot: &service::Snapshot<service::Endpoint<I, A>, N>, metadata: &M);
    fn expired(&mut self, revision: service::Revision);
    fn reconciled(&mut self, revision: service::Revision, change: service::Change);
    fn failed(&mut self, error: &E);
    fn rejected(&mut self, error: service::ReconcileError);
}

/// An explicit observer for callers that deliberately discard discovery
/// telemetry.
pub struct Ignore;

impl<I, A, M, E, const N: usize> Observe<I, A, M, E, N> for Ignore {
    fn resolved(
        &mut self,
        _snapshot: &service::Snapshot<service::Endpoint<I, A>, N>,
        _metadata: &M,
    ) {
    }

    fn expired(&mut self, _revision: service::Revision) {}

    fn reconciled(&mut self, _revision: service::Revision, _change: service::Change) {}

    fn failed(&mut self, _error: &E) {}

    fn rejected(&mut self, _error: service::ReconcileError) {}
}
