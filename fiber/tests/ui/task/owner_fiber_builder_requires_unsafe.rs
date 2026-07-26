use std::convert::Infallible;

use dope_fiber::abi::pending;
use dope_fiber::owner::{FiberScope, OwnerFiber, SplitBytes};
use dope_test::with_session;
use o3::buffer::Shared;

fn main() {
    with_session(|session| {
        let owner = SplitBytes::new(Shared::copy_from_slice(b"request"), None, 7);
        let _ =
            OwnerFiber::try_from_split(owner, FiberScope::from_driver(session.driver()), |_| {
                Ok::<_, Infallible>(pending::<()>())
            });
    });
}
