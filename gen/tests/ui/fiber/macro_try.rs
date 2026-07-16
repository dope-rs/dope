extern crate dope;

fn main() {
    let _ = dope_gen::fiber!('_ => async move {
        ::core::matches!(Ok::<usize, ()>(1)?, Ok(_))
    });
}
