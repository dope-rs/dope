extern crate dope;

fn main() {
    let _ = dope_gen::fiber!('_ => async move {
        std::future::ready(()).await;
    });
}
