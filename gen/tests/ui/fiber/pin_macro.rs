extern crate dope;

#[expect(unused_imports)]
use dope_fiber::abi::ready::Ready;

fn main() {
    let _ = dope_gen::fiber!('_ => async move {
        let _value = ::std::pin::pin!(Ready::new(()));
    });
}
