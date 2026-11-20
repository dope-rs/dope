mod budget;
pub mod counter;
pub mod fairness;
mod text;

pub use budget::Budget;
use counter::Counter;
use engine::core::driver::route::KeyTag;
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
pub struct Forward {
    #[pin]
    #[forward]
    pub inner: Counter<KeyTag<6>>,
}

#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine)]
pub struct Nested {
    #[pin]
    #[manifold]
    pub inner: Forward,
}
