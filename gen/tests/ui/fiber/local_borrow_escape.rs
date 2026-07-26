extern crate dope;

fn leak<'d>() -> impl dope_fiber::abi::Fiber<'d, Output = &'d str> {
    dope_gen::fiber!('d => async move {
        let text = dope_fiber::abi::ready(String::from("owned")).await;
        let borrowed = dope_fiber::abi::ready(text.as_str()).await;
        let _owner_remains_live = text.len();
        borrowed
    })
}

fn main() {
    let _ = leak();
}
