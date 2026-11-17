use o3::mem::slot;
use std::task;

/// Host authority and branded outputs consumed by one readiness dispatch.
pub trait Context<H> {
    type Backend;
    type Event;
    type Failure;
    type Dispatch: From<(task::Poll<()>, Option<Self::Event>)>
        + From<(task::Poll<()>, Self::Failure)>;

    fn backend(host: &mut H) -> &mut Self::Backend;
    fn resolve<'event>(
        host: &mut H,
        output: slot::Vacant<'event, Self::Event>,
    ) -> Option<Result<Option<slot::Occupied<'event, Self::Event>>, Self::Failure>>;
}
