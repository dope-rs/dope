use std::mem::size_of;

use dope_core::driver::route::KeyTag;
use dope_manifold::client::{conn, side::Primary};
use dope_net::wire::Identity;

#[test]
fn disabled_layout() {
    type Owner = Primary<'static, KeyTag<0>>;
    type Ctx = conn::Ctx<'static, 'static, KeyTag<0>, Identity, (), Owner>;
    type Ref = conn::Ref<'static, 'static, KeyTag<0>, Identity, (), Owner>;

    assert_eq!(size_of::<Ctx>(), 7 * size_of::<usize>());
    assert_eq!(size_of::<Ref>(), 3 * size_of::<usize>());
}
