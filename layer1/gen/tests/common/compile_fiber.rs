struct __DopeScope(u8);
use fibers as __DopeScope_;

fn main() {
    let _ = dope_gen::fiber!('_, crate = __DopeScope_ => async move {
        let value: __DopeScope = __DopeScope(7);
        fibers::abi::Ready::new(value.0).await;
    });
}
