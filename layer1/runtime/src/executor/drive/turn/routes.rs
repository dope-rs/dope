use dope_core::driver::route::{self, kind};

/// Executor wake route, kept nominally distinct from other framework one-shots.
#[derive(Clone, Copy)]
pub(crate) enum Wake {}

impl route::Tag for Wake {
    const ROUTE: route::Route = <route::FrameworkTag<kind::OneShot> as route::Tag>::ROUTE;
    type Class = kind::OneShot;
}
