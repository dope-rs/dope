extern crate dope;

fn main() {
    let _ = dope_gen::fiber!('_ => async move {
        ::core::matches!(return 1usize, _)
    });
}
