use dope_fiber::{FiberScope, OwnerFiber, Pending, SplitBytes};
use dope_test::with_session;
use o3::buffer::Shared;

fn main() {
    with_session(|session| {
        let owner = SplitBytes::new(Shared::copy_from_slice(b"request"), None, 7);
        let _ =
            OwnerFiber::try_from_split(owner, FiberScope::from_driver(session.driver()), |view| {
                Err::<Pending<()>, _>(view.head())
            });
    });
}
