use std::mem::{align_of, needs_drop, size_of};

pub mod common;

use common::wire::Failing;
use dope_core::driver::route;
use dope_net::{
    link::{
        egress::{self, metadata},
        pool::recv::{input, storage::tail},
    },
    wire::{
        self, failure, open,
        receive::{self, batch::One},
        send::Vectored,
    },
};
use metadata::{batch, front, reserve};
use o3::{buffer::bytes::Source, mem};
use wire::{
    receive::mode,
    send::{Empty, Transition, reclaim},
    terminal,
};

#[test]
fn failure_markers_are_zst() {
    assert_eq!(size_of::<failure::Open>(), 0);
    assert_eq!(size_of::<failure::Terminal>(), 0);
    assert_eq!(size_of::<failure::Possible>(), 0);
    assert_eq!(size_of::<failure::Never>(), 0);
}

#[test]
fn failure_exact_layout() {
    assert_eq!(
        size_of::<open::Error<'static, Failing>>(),
        size_of::<usize>()
    );
    assert_eq!(
        align_of::<open::Error<'static, Failing>>(),
        align_of::<usize>()
    );
    assert_eq!(
        size_of::<terminal::Error<'static, Failing>>(),
        size_of::<usize>() + size_of::<Option<usize>>()
    );
    assert_eq!(
        align_of::<terminal::Error<'static, Failing>>(),
        align_of::<usize>()
    );
    assert_eq!(size_of::<open::Error<'static, wire::Identity>>(), 0);
    assert_eq!(size_of::<terminal::Error<'static, wire::Identity>>(), 0);
}

#[test]
fn failure_state_two_words() {
    assert!(size_of::<terminal::State<usize>>() <= 2 * size_of::<usize>());
}

#[test]
fn credit_selects_layout() {
    assert_eq!(
        size_of::<<mode::Disabled as mode::Credit>::Guard<'static>>(),
        0
    );
    assert!(!needs_drop::<
        <mode::Disabled as mode::Credit>::Guard<'static>,
    >());
    assert_eq!(
        size_of::<<mode::Enabled as mode::Credit>::Guard<'static>>(),
        2 * size_of::<usize>()
    );
    assert!(needs_drop::<<mode::Enabled as mode::Credit>::Guard<'static>>());
}

#[test]
fn vectored_caches_length() {
    assert_eq!(size_of::<Vectored<'static>>(), 4 * size_of::<usize>());
    assert_eq!(
        size_of::<Vectored<'static, 1>>(),
        size_of::<Vectored<'static>>()
    );
    assert_eq!(
        size_of::<Vectored<'static, 32>>(),
        size_of::<Vectored<'static>>()
    );
}

#[test]
fn send_proof_layouts() {
    assert!(size_of::<Transition<'static, reclaim::OnSubmit, Empty>>() <= size_of::<usize>());
}

#[test]
fn tail_proof_layouts() {
    assert_eq!(
        size_of::<<input::Borrowed as input::Kind<Failing>>::Tail<'static, 'static>>(),
        0
    );
    assert_eq!(
        align_of::<<input::Borrowed as input::Kind<Failing>>::Tail<'static, 'static>>(),
        1
    );
    assert_eq!(
        size_of::<
            tail::Table<
                (),
                route::KeyTag<0>,
                <input::Borrowed as input::Kind<Failing>>::Tail<'static, 'static>,
            >,
        >(),
        size_of::<route::table::Slab<(), route::KeyTag<0>>>()
    );
    assert_eq!(
        align_of::<
            tail::Table<
                (),
                route::KeyTag<0>,
                <input::Borrowed as input::Kind<Failing>>::Tail<'static, 'static>,
            >,
        >(),
        align_of::<route::table::Slab<(), route::KeyTag<0>>>()
    );
    assert_eq!(
        size_of::<tail::Cleared<'static, (), route::KeyTag<0>>>(),
        2 * size_of::<usize>()
    );
    assert_eq!(
        align_of::<tail::Cleared<'static, (), route::KeyTag<0>>>(),
        align_of::<usize>(),
    );
    assert_eq!(
        size_of::<Option<tail::Cleared<'static, (), route::KeyTag<0>>>>(),
        2 * size_of::<usize>()
    );
    assert_eq!(
        size_of::<tail::Slot<'static, (), route::KeyTag<0>, ()>>(),
        3 * size_of::<usize>()
    );
}

#[test]
fn receive_loan_layouts() {
    use dope_net::link::pool::recv::storage;
    use storage::raw::Mode;

    assert_eq!(size_of::<<mode::Disabled as Mode>::Storage<'static>>(), 0);
    assert_eq!(
        size_of::<<mode::Disabled as Mode>::Bound<'static, 'static>>(),
        size_of::<usize>()
    );
    assert_eq!(
        size_of::<<mode::Parking as Mode>::Bound<'static, 'static>>(),
        2 * size_of::<usize>()
    );
    assert_eq!(
        size_of::<<mode::Enabled as Mode>::Bound<'static, 'static>>(),
        3 * size_of::<usize>()
    );
    assert_eq!(
        size_of::<Option<storage::Loan<'static, 'static>>>(),
        size_of::<storage::Loan<'static, 'static>>()
    );
    assert_eq!(
        size_of::<tail::Table<(), route::KeyTag<0>, (), storage::Parking<'static>>>(),
        size_of::<route::table::Slab<(), route::KeyTag<0>>>()
            + size_of::<storage::Parking<'static>>()
    );
    assert_eq!(
        size_of::<tail::Table<(), route::KeyTag<0>, (), storage::Retained<'static>>>(),
        size_of::<route::table::Slab<(), route::KeyTag<0>>>()
            + size_of::<storage::Retained<'static>>()
    );
}

#[test]
fn metadata_owner_layouts() {
    assert_eq!(size_of::<egress::NonEmpty<()>>(), 8);
    assert_eq!(
        size_of::<metadata::Queue<'static, 'static, 'static, ()>>(),
        2 * size_of::<usize>()
    );
    assert_eq!(
        size_of::<front::Front<'static, 'static, 'static, 'static, ()>>(),
        4 * size_of::<usize>()
            + size_of::<mem::raw::Grant<mem::raw::Lane<'static, 'static, 2, (), u32>, 2>>()
            + size_of::<egress::NonEmpty<()>>()
    );
    assert_eq!(size_of::<metadata::batch::Plan>(), size_of::<usize>());
    assert_eq!(
        size_of::<batch::Sink<'static, 'static, 'static, 'static, 'static, ()>>(),
        size_of::<usize>()
    );
    assert_eq!(
        size_of::<reserve::Reserve<'static, 'static, 'static, 'static, ()>>(),
        4 * size_of::<usize>()
            + size_of::<mem::raw::Grant<mem::raw::Lane<'static, 'static, 2, (), u32>, 2>>()
            + size_of::<egress::NonEmpty<()>>()
    );
}

#[test]
fn mixed_receive_pays_sum() {
    fn batch<S: for<'input> receive::raw::Batch<'input>>() {}

    batch::<One<&'static [u8]>>();
    batch::<One<Source<'static, &'static [u8]>>>();

    assert_eq!(size_of::<One<&'static [u8]>>(), size_of::<&'static [u8]>());
    assert_eq!(
        size_of::<One<Source<'static, &'static [u8]>>>(),
        size_of::<Source<'static, &'static [u8]>>()
    );
}
