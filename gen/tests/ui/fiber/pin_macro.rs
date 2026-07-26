extern crate dope;

fn main() {
    let _ = dope_gen::fiber!('_ => async move {
        let _value = ::std::pin::pin!(dope_fiber::abi::ready(()));
    });
}
