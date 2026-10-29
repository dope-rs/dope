use engine::{core::driver::route::KeyTag, manifold::timing::interval::Interval};

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(crate = ::engine)]
struct App<'d> {
    #[manifold]
    interval: Interval<'d, KeyTag<1>>,
}

fn main() {}
