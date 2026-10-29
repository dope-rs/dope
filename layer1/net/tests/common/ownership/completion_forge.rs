use dope_core::io::event::net::recv;
use dope_net::link::event::Completion;

fn forge<'d>(done: recv::Completion<'d>) -> Completion<'d> {
    Completion(done)
}

fn main() {}
