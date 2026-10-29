use dope_core::driver::route::KeyTag;
pub mod common;

use std::mem::{size_of, size_of_val};

use dope::{
    core::driver::route::Token,
    manifold::client::conn,
    net::wire::{
        Identity,
        receive::{Chunk, mode::Disabled},
    },
};
use dope_fiber::{
    abi::Ready,
    context::{PollCall, wake},
    net::{Io, read},
    task::storage::{fixed::staged, set::Set},
};

use crate::common::storage::cases::Domain;

#[test]
#[deny(unsafe_code)]
fn keeps_hot_layouts() {
    use dope_fiber::abi::future::raw::{Brand, Seal};

    assert_eq!(size_of::<Brand<'static, Domain>>(), 0);
    assert_eq!(size_of::<Seal<'static, Domain>>(), 0);
    let native = async move { [0_u8; 13] };
    let fiber = dope_fiber::fiber!('_ => async move { [0_u8; 13] });
    assert_eq!(size_of_val(&fiber), size_of_val(&native));
    assert_eq!(
        size_of::<dope_fiber::abi::Task<dope_fiber::abi::Pending<()>>>(),
        1
    );
    assert_eq!(
        size_of::<Io<'static, 'static, 'static, Identity, conn::Id<'static, KeyTag<0>>>>(),
        size_of::<(&'static (), Token)>(),
    );
    assert_eq!(
        size_of::<read::Lease<'static, 'static, 'static, Identity>>(),
        size_of::<Chunk<'static, 'static, Identity, Disabled>>(),
    );
    assert_eq!(
        size_of::<wake::RootWaker<'static>>(),
        size_of::<[usize; 2]>()
    );
    assert_eq!(size_of::<wake::Waker<'static>>(), size_of::<[usize; 2]>());
    assert_eq!(
        size_of::<Option<wake::RootWaker<'static>>>(),
        size_of::<[usize; 2]>()
    );
    assert_eq!(
        size_of::<PollCall<'static, 'static, 'static, Ready<()>>>(),
        size_of::<[usize; 2]>()
    );
    assert_eq!(
        size_of::<staged::Slab<'static, Ready<()>, 1, Domain>>(),
        size_of::<[usize; 4]>(),
    );
    assert_eq!(
        size_of::<Set<'static, Ready<()>, (), Domain>>(),
        size_of::<usize>(),
    );
}
