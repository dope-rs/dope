mod budget;
pub mod counter;
pub mod fairness;
mod text;

pub use budget::Budget;
use counter::Counter;
use engine::{
    core::driver::route::{Identity, KeyTag},
    manifold::timing::interval,
};
pub use text::Text;

#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine)]
pub struct Optional {
    #[pin]
    #[manifold(optional)]
    pub optional: Option<Counter<KeyTag<3>>>,
}

#[pin_project::pin_project]
#[derive(engine::Application, Default)]
#[dispatcher(crate = ::engine)]
pub struct Dispatcher {
    #[pin]
    #[manifold]
    pub a: Counter<KeyTag<3>>,
    #[pin]
    #[manifold]
    pub b: Counter<KeyTag<0>>,
    #[pin]
    #[manifold]
    pub c: Counter<KeyTag<1>>,
}

#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine, driver = 'd)]
pub struct Branded<'d> {
    #[pin]
    #[manifold]
    pub inner: Counter<KeyTag<5>, &'d ()>,
}

#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine, driver = 'd)]
pub struct Selected<'scope, 'd> {
    #[pin]
    #[manifold]
    pub inner: Counter<KeyTag<8>, (&'scope (), &'d ())>,
}

#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine)]
pub struct Stateful {
    #[pin]
    #[manifold]
    pub inner: Counter<KeyTag<4>>,
    #[dispatcher(state)]
    pub state: usize,
}

#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine)]
pub struct Metered {
    #[pin]
    #[manifold]
    pub a: Budget<KeyTag<3>>,
    #[pin]
    #[manifold]
    pub b: Budget<KeyTag<0>>,
    #[pin]
    #[manifold]
    pub c: Budget<KeyTag<1>>,
}

#[pin_project::pin_project]
#[derive(engine::Forward)]
#[forward(crate = ::engine)]
pub struct Scoped<'scope, 'd> {
    #[pin]
    #[forward('d)]
    pub inner: Counter<KeyTag<7>, (&'scope (), &'d ())>,
}

#[pin_project::pin_project]
#[derive(engine::Forward)]
#[forward(crate = ::engine)]
pub struct Forward {
    #[pin]
    #[forward]
    pub inner: Counter<KeyTag<6>>,
}

#[pin_project::pin_project]
#[derive(engine::Forward)]
#[forward(crate = ::engine)]
pub struct Retained<'d> {
    #[pin]
    #[forward]
    pub inner: interval::Interval<'d, KeyTag<10>>,
}

#[pin_project::pin_project]
#[derive(engine::Forward)]
#[forward(crate = ::engine)]
pub struct Defaulted<'d, M = (), ID: Identity = KeyTag<9>> {
    #[pin]
    #[forward('d)]
    pub inner: Counter<ID, (&'d (), M)>,
}

#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine)]
pub struct Lifetime<'d> {
    #[pin]
    #[manifold]
    pub inner: interval::Interval<'d, KeyTag<11>>,
}

#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine)]
pub struct Nested {
    #[pin]
    #[manifold]
    pub inner: Forward,
}

#[derive(engine::Application)]
#[dispatcher(crate = ::engine)]
pub struct Facade {}

#[derive(engine::Application)]
#[dispatcher(
    core = ::engine::core,
    manifold = ::engine::manifold,
    runtime = ::engine::runtime
)]
pub struct Parts {}
