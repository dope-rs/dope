extern crate dope;

fn main() {
    let _ = dope_gen::fiber!('_ => async move {
        let _ = async || dope_fiber::ready(()).await;
    });
}
