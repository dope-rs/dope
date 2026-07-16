extern crate dope;

fn leak<'d>(text: String) -> impl dope_fiber::Fiber<'d, Output = &'d str> {
    dope_gen::fiber!('d => async move {
        dope_fiber::ready((&text).as_str()).await
    })
}

fn main() {
    let _ = leak(String::from("owned"));
}
