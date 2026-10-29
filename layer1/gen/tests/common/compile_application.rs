use engine::{core::driver::route::KeyTag, manifold::timing::interval::Interval};

#[pin_project::pin_project]
#[derive(dope_gen::Application, dope_gen::Forward)]
#[dispatcher(crate = ::engine)]
#[forward(crate = ::engine,)]
struct App<'d> {
    #[pin]
    #[manifold]
    #[forward]
    interval: Interval<'d, KeyTag<1>>,
}

#[derive(dope_gen::WorkGraph)]
#[work(core = ::engine::core, runtime = ::engine::runtime)]
enum r#T {
    r#into_work,
    Next,
}

fn main() {}
