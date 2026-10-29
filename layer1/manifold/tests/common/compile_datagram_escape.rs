use dope_core::driver::route::KeyTag;
use dope_manifold::network::datagram::View;

fn escape<'a, 'b: 'a, 'd: 'b>(view: View<'a, 'd, KeyTag<0>, u32, ()>) -> &'b u32 {
    view.read(|handler| handler)
}

fn main() {}
