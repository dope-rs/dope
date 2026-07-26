extern crate dope;

use dope_fiber::abi::ready::Ready;

fn leak<'d>() -> impl dope_fiber::abi::Fiber<'d, Output = &'d str> {
    dope_gen::fiber!('d => async move {
        let text = Ready::new(String::from("owned")).await;
        let borrowed = Ready::new(text.as_str()).await;
        let _owner_remains_live = text.len();
        borrowed
    })
}

fn main() {
    let _ = leak();
}
