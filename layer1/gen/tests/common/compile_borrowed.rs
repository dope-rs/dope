use std::pin::Pin;

#[derive(dope_gen::Application)]
#[dispatcher(crate = ::engine)]
struct Owner {}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(crate = ::engine)]
struct App<'app> {
    #[pin]
    #[app(borrowed)]
    owner: Pin<&'app mut Owner>,
}

fn main() {}
