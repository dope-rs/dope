extern crate dope;

#[dope_gen::fiber_fn('d)]
async fn run<'d>() {
    let _ = async {};
}

fn main() {}
