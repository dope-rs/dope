extern crate dope;

fn leak<'d>() -> impl dope_fiber::Fiber<'d, Output = &'d str> {
    dope_gen::fiber!('d => async move {
        let text = dope_fiber::ready(String::from("owned")).await;
        let borrowed = dope_fiber::ready(text.as_str()).await;
        let _owner_remains_live = text.len();
        borrowed
    })
}

fn main() {
    let _ = leak();
}
