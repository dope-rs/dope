extern crate dope;

#[expect(unused_imports)]
use dope_fiber::abi::ready::Ready;

fn main() {
    let _ = dope_gen::fiber!('_ => async move {
        async fn escaped() {
            Ready::new(()).await;
        }
        trait Trait {
            async fn escaped();
        }
        struct Type;
        impl Trait for Type {
            async fn escaped() {
                Ready::new(()).await;
            }
        }
        let _ = async { Ready::new(()).await };
    });
}
