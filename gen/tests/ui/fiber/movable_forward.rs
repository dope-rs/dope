use std::pin::Pin;

extern crate dope;
use dope::DriverContext;
use dope::manifold::Manifold;

struct Inner;

impl<'d> Manifold<'d> for Inner {
    const ID: u8 = 0;

    fn pre_park(self: Pin<&mut Self>, _: &mut DriverContext<'_, 'd>) {}
}

#[pin_project::pin_project]
#[derive(dope_gen::Forward)]
struct App {
    #[forward]
    inner: Inner,
}

fn main() {
    let _ = Pin::new(Box::new(App { inner: Inner }));
}
