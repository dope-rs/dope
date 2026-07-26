extern crate dope;

#[expect(unused_imports)]
use dope_fiber::abi::ready::Ready;

fn main() {
    let _ = dope_gen::fiber!('_ => async move {
        let _ = async || Ready::new(()).await;
    });
}
