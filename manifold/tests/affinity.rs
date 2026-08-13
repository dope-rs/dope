use std::mem::size_of;

use dope_manifold::connector::{
    attempt::Id,
    auxiliary::{Disabled, Primary},
    connection,
};
use dope_net::wire::Identity;
use dope_test::checks::affinities::Affinity;

const _: fn() = || {
    Affinity::<Id<'static>>::not_send::<_>();
    Affinity::<Id<'static>>::not_sync::<_>();
};

#[test]
fn attempt_ids_keep_their_compact_layout() {
    assert_eq!(size_of::<Id<'static>>(), 2 * size_of::<u32>());
}

#[test]
fn disabled_auxiliary_mode_preserves_primary_callback_layouts() {
    type Owner = Primary<'static, 0>;
    type Ctx = connection::Ctx<'static, 'static, 0, Identity, (), Owner>;
    type Ref = connection::Ref<'static, 'static, 0, Identity, (), Owner>;

    assert_eq!(size_of::<Disabled>(), 0);
    assert_eq!(size_of::<Owner>(), size_of::<Id<'static>>());
    assert_eq!(size_of::<Ctx>(), 2 * size_of::<usize>());
    assert_eq!(size_of::<Ref>(), size_of::<usize>());
}
