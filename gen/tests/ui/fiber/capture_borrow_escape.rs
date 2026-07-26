extern crate dope;

use dope_fiber::abi::ready::Ready;

fn leak<'d>(text: String) -> impl dope_fiber::abi::Fiber<'d, Output = &'d str> {
    dope_gen::fiber!('d => async move {
        Ready::new((&text).as_str()).await
    })
}

fn main() {
    let _ = leak(String::from("owned"));
}
