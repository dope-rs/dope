extern crate dope;

#[repr(packed)]
#[derive(dope_gen::Dispatcher)]
struct App {
    #[manifold]
    inner: (),
}

fn main() {}
