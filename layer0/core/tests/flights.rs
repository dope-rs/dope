use o3::depot::arena::slab::external::Generation as _;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();
use std::mem;

use dope_core::driver::{
    retained::flight::storage,
    route::{indices, table::Parts},
};
use dope_test::alloc::Tracker;
use o3::alloc;

#[test]
fn preserves_contracts() {
    assert_eq!(
        mem::size_of::<storage::Flights<u64, 24, 32>>(),
        4 * mem::size_of::<usize>()
    );
    assert_eq!(
        mem::align_of::<storage::Flights<u64, 24, 32>>(),
        mem::align_of::<usize>()
    );
    assert_eq!(
        mem::size_of::<storage::Builder<'static, u64, 24, 32>>(),
        mem::size_of::<usize>()
    );
    assert_eq!(
        mem::size_of::<storage::Echo<u64, 24, 32>>(),
        mem::size_of::<u64>()
    );
    assert_eq!(
        mem::size_of::<Option<storage::Echo<u64, 24, 32>>>(),
        mem::size_of::<u64>()
    );
    assert_eq!(
        mem::size_of::<storage::Reserved<'static, u64, 24, 32>>(),
        mem::size_of::<usize>()
    );
    assert_eq!(
        mem::size_of::<storage::Lease<'static, u64, 24, 32>>(),
        mem::size_of::<usize>()
    );
    assert_eq!(
        mem::size_of::<Option<storage::Lease<'static, u64, 24, 32>>>(),
        mem::size_of::<usize>()
    );
    assert_eq!(
        mem::size_of::<storage::Slots<'static, u64, 24, 32>>(),
        mem::size_of::<usize>()
    );
    assert_eq!(
        mem::size_of::<storage::Vacant<'static, 'static, u64, 24, 32>>(),
        2 * mem::size_of::<usize>()
    );
    assert_eq!(
        mem::size_of::<storage::Drain<'static, u64, 24, 32>>(),
        mem::size_of::<usize>()
    );

    assert!(storage::Echo::<u64, 24, 32>::from_exposed(0).is_none());
    assert!(storage::Echo::<u64, 24, 32>::from_exposed(1).is_none());
    assert!(storage::Echo::<u64, 24, 32>::from_exposed(1 << 56).is_none());
    assert!(storage::Echo::<u64, 24, 32>::from_exposed((1 << 56) - 1).is_some());

    let mut arena = storage::Flights::<u64, 24, 32>::new();
    let mut builder = arena.builder();
    let prepare = Tracker::begin();
    let prepared = (builder.try_batch([2])).expect("the fresh two-slot flight group must prepare");
    assert_eq!(
        prepare.finish().allocations,
        2,
        "a fresh flight group allocates its block and reserved directory"
    );
    let commit = Tracker::begin();
    {
        let [slots] = prepared.groups();
        assert_eq!(
            commit.finish().allocations,
            0,
            "flight commit must not allocate"
        );

        let stale = {
            let vacant =
                (slots.try_vacancy()).expect("a fresh two-slot group exposes its first vacancy");
            let rolled = vacant.reserve(7);
            rolled.echo()
        };
        assert_eq!(
            builder.drain().bind(stale).map(|bound| bound.complete()),
            None
        );

        let tracker = Tracker::begin();
        let vacant = (slots.try_vacancy()).expect("dropping a reservation restores its vacancy");
        let held = vacant.reserve(11);
        let echo = held.echo();
        let back = (storage::Echo::<u64, 24, 32>::from_exposed(echo.expose()))
            .expect("a live reservation has a valid echo encoding");
        assert!(back == echo);
        {
            let lease = held.commit();
            assert_eq!(lease.value(), 11);
            assert_eq!(
                builder.drain().bind(back).map(|bound| bound.complete()),
                Some(11)
            );
            assert_eq!(lease.value(), 11);
        }
        assert_eq!(
            tracker.finish().allocations,
            0,
            "flight operations must not allocate"
        );

        let fresh = {
            let vacant = (slots.try_vacancy()).expect("terminal completion releases its slot");
            let next = vacant.reserve(13).commit();
            next.echo()
        };
        assert!(fresh != back);
        assert_eq!(
            builder.drain().bind(back).map(|bound| bound.complete()),
            None
        );
        assert_eq!(
            builder.drain().bind(fresh).map(|bound| bound.value()),
            Some(13)
        );
        assert_eq!(
            builder.drain().bind(fresh).map(|bound| bound.value()),
            Some(13)
        );
        assert_eq!(
            builder.drain().bind(fresh).map(|bound| bound.complete()),
            Some(13)
        );
        assert_eq!(
            builder.drain().bind(fresh).map(|bound| bound.complete()),
            None
        );

        let echo = {
            let vacant = (slots.try_vacancy()).expect("the released group retains a vacant slot");
            let lease = vacant.reserve(17).commit();
            lease.echo()
        };
        assert_eq!(
            builder.drain().bind(echo).map(|bound| bound.complete()),
            Some(17)
        );
    }

    let forged = (storage::Echo::<u64, 24, 32>::from_exposed((1u64 << 24) | ((1u64 << 24) - 1)))
        .expect("a nonzero generation and a 24-bit index form a valid 56-bit echo");
    assert_eq!(
        builder.drain().bind(forged).map(|bound| bound.complete()),
        None
    );
}

#[test]
fn batches_before_commit() {
    let mut arena = storage::Flights::<u64, 24, 32>::new();
    let mut builder = arena.builder();
    {
        let prepare = Tracker::begin();
        let prepared =
            (builder.try_batch([1, 2])).expect("the two-group flight batch must prepare");
        assert_eq!(
            prepare.finish().allocations,
            3,
            "a fresh batch allocates two blocks and one shared directory"
        );
        let commit = Tracker::begin();
        let [first, second] = prepared.groups();
        assert_eq!(
            commit.finish().allocations,
            0,
            "batch commit must not allocate"
        );
        let left = (first.try_vacancy()).expect("the first group must expose its slot");
        let right = (second.try_vacancy()).expect("the second group must expose its first slot");
        let left = left.reserve(29);
        let right = right.reserve(31);
        assert!(left.echo() != right.echo());
    }

    let prepare = Tracker::begin();
    let prepared =
        (builder.try_batch([2, 1])).expect("the released groups must prepare as one batch");
    assert_eq!(
        prepare.finish().allocations,
        0,
        "a batch reuses distinct released groups"
    );
    let [first, second] = prepared.groups();
    assert!(first.try_vacancy().is_ok());
    assert!(second.try_vacancy().is_ok());
}

#[test]
fn retires_exhausted_echo() {
    let mut arena = storage::Flights::<u64, 32, 1>::new();
    let mut builder = arena.builder();
    let old_echo = {
        let prepared = (builder.try_batch([1])).expect("the one-slot flight group must prepare");
        let [slots] = prepared.groups();
        let vacant = (slots.try_vacancy()).expect("a one-slot group exposes its only vacancy");
        let retired = vacant.reserve(19);
        let old_echo = retired.echo();
        let retired = retired.commit();
        assert_eq!(
            builder.drain().bind(old_echo).map(|bound| bound.complete()),
            Some(19)
        );
        assert_eq!(retired.value(), 19);
        old_echo
    };

    let prepared = (builder.try_batch([1])).expect("the successor flight group must prepare");
    let [slots] = prepared.groups();
    let vacant = (slots.try_vacancy()).expect("a successor group exposes its only vacancy");
    let next = vacant.reserve(23);
    assert!(next.echo() != old_echo);
    assert_eq!(
        builder.drain().bind(old_echo).map(|bound| bound.complete()),
        None
    );
}

#[test]
fn rejects_invalid_widths() {
    let mut arena = storage::Flights::<u64, 0, 32>::new();
    assert!(matches!(
        arena.builder().try_batch([1]),
        Err(alloc::Error::Overflow)
    ));

    let mut arena = storage::Flights::<u64, 32, 33>::new();
    assert!(matches!(
        arena.builder().try_batch([1]),
        Err(alloc::Error::Overflow)
    ));
}

#[test]
fn native_route_encoding() {
    use dope_core::driver::{route, route::kind};

    fn round_trip<C: kind::Flight>() {
        let epoch = route::Epoch::INITIAL.next().expect("second epoch");
        let parts = Parts::<route::KeyTag<17, C>>::new(indices::SlotIndex::MAX, epoch);
        let token = route::Token::from_parts(parts);
        assert_eq!(token.kind(), Ok(C::KIND));
        assert_eq!(token.slot(), indices::SlotIndex::MAX);
        assert_eq!(token.epoch(), epoch);
        assert_eq!(C::FLIGHT as u8, C::KIND as u8);
    }

    round_trip::<kind::Accept>();
    round_trip::<kind::Recv>();
    round_trip::<kind::Send>();
    round_trip::<kind::Socket>();
    round_trip::<kind::Connect>();
    round_trip::<kind::Open>();
    round_trip::<kind::Tuning>();
    round_trip::<kind::Write>();
    round_trip::<kind::Sync>();
}
