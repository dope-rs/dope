use std::task;

/// Host authority and branded outputs consumed by one readiness dispatch.
pub trait Context<H> {
    type Backend;
    type Event;
    type Failure;
    type Dispatch: From<(task::Poll<()>, Option<Self::Event>)>
        + From<(task::Poll<()>, Self::Failure)>;

    fn backend(host: &mut H) -> &mut Self::Backend;
    fn resolve(host: &mut H) -> Option<Result<Option<Self::Event>, Self::Failure>>;
}
