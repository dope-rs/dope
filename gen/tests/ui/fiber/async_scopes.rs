extern crate dope;

fn main() {
    let _ = dope_gen::fiber!('_ => async move {
        async fn escaped() {
            dope_fiber::ready(()).await;
        }
        trait Trait {
            async fn escaped();
        }
        struct Type;
        impl Trait for Type {
            async fn escaped() {
                dope_fiber::ready(()).await;
            }
        }
        let _ = async { dope_fiber::ready(()).await };
    });
}
