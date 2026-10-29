use std::task;

/// Host authority and branded outputs consumed by one readiness dispatch.
pub trait Context<H> {
    type Backend;
    type Event;
    type Failure;
    type Dispatch: From<(task::Poll<()>, Option<Self::Event>)>
        + From<(task::Poll<()>, Self::Failure)>;

    fn with<R>(host: &mut H, run: impl FnOnce(&mut Self::Backend) -> R) -> R;
    fn resolve(host: &mut H) -> Option<Result<Option<Self::Event>, Self::Failure>>;
}
