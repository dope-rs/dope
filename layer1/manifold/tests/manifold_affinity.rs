use std::mem::size_of;

use dope_core::driver::route::KeyTag;
use dope_manifold::{
    client::{
        attempt::{Completion, Id},
        conn,
        side::Primary,
    },
    network::datagram::{Socket, group, packet},
};
use dope_net::wire::Identity;

trait MaybeSend<A> {}
impl<T: ?Sized> MaybeSend<()> for T {}
impl<T: ?Sized + Send> MaybeSend<u8> for T {}

trait MaybeSync<A> {}
impl<T: ?Sized> MaybeSync<()> for T {}
impl<T: ?Sized + Sync> MaybeSync<u8> for T {}

fn confined<T: ?Sized + MaybeSend<A> + MaybeSync<B>, A, B>() {}

const _: fn() = || {
    confined::<Id<'static>, _, _>();
    confined::<Completion<'static, 'static>, _, _>();
    confined::<packet::Packet<'static, 'static, ()>, _, _>();
    confined::<packet::Frozen<'static, 'static, ()>, _, _>();
    confined::<packet::Retainer<'static>, _, _>();
    confined::<Socket<'static, KeyTag<0>>, _, _>();
    confined::<group::Access<'static, 'static, KeyTag<0>>, _, _>();
};

#[test]
fn attempt_layout() {
    assert_eq!(size_of::<Id<'static>>(), 2 * size_of::<u32>());
}

#[test]
fn disabled_layout() {
    type Owner = Primary<'static, KeyTag<0>>;
    type Ctx = conn::Ctx<'static, 'static, KeyTag<0>, Identity, (), Owner>;
    type Ref = conn::Ref<'static, 'static, KeyTag<0>, Identity, (), Owner>;

    assert_eq!(size_of::<Ctx>(), 7 * size_of::<usize>());
    assert_eq!(size_of::<Ref>(), 3 * size_of::<usize>());
}
