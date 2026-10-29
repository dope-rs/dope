use dope_core::driver::schedule::ready::wake::fanout::storage;

#[test]
fn publish_during_drain() {
    let book = storage::Registry::try_new(3).expect("fanout capacity");
    let mut first = storage::Lease::new();
    let mut second = storage::Lease::new();
    let mut later = storage::Lease::new();
    book.try_keep(&mut first, 1).expect("first");
    book.try_keep(&mut second, 2).expect("second");
    book.publish();
    assert!(matches!(book.step(), Some(storage::Step::Item(1))));
    book.publish();
    book.try_keep(&mut first, 11).expect("rearm");
    book.publish();
    book.try_keep(&mut later, 3).expect("later");
    assert!(matches!(book.step(), Some(storage::Step::Item(2))));
    assert!(matches!(book.step(), Some(storage::Step::Idle)));
    assert!(matches!(book.step(), Some(storage::Step::Item(11))));
    assert!(matches!(book.step(), Some(storage::Step::Idle)));
    assert!(matches!(book.step(), Some(storage::Step::Idle)));
    assert!(book.step().is_none());
}

#[test]
fn broadcasts_each_epoch() {
    let book = (storage::Registry::try_new(2)).expect("the fanout registry must admit two leases");
    let mut first = storage::Lease::new();
    let mut second = storage::Lease::new();

    book.try_keep(&mut first, 1).expect("fanout registration");
    book.try_keep(&mut second, 2).expect("fanout registration");
    book.try_keep(&mut first, 3).expect("fanout registration");
    book.publish();
    assert!(matches!(book.step(), Some(storage::Step::Item(3))));
    assert!(matches!(book.step(), Some(storage::Step::Item(2))));
    assert!(book.step().is_none());

    book.publish();
    assert!(matches!(book.step(), Some(storage::Step::Idle)));
    assert!(matches!(book.step(), Some(storage::Step::Idle)));
    assert!(book.step().is_none());
    book.try_keep(&mut first, 4).expect("fanout registration");
    book.publish();
    assert!(matches!(book.step(), Some(storage::Step::Item(4))));
    assert!(matches!(book.step(), Some(storage::Step::Idle)));
    assert!(book.step().is_none());
}

#[test]
fn skips_current_epoch() {
    let book = (storage::Registry::try_new(2)).expect("the fanout registry must admit two leases");
    let mut first = storage::Lease::new();
    let mut second = storage::Lease::new();

    book.try_keep(&mut first, 1).expect("fanout registration");
    book.publish();
    book.try_keep(&mut second, 2).expect("fanout registration");
    assert!(matches!(book.step(), Some(storage::Step::Item(1))));
    assert!(matches!(book.step(), Some(storage::Step::Idle)));
    assert!(book.step().is_none());

    book.publish();
    assert!(matches!(book.step(), Some(storage::Step::Idle)));
    assert!(matches!(book.step(), Some(storage::Step::Item(2))));
}

#[test]
fn drop_advances_cursor() {
    let book = (storage::Registry::try_new(2)).expect("the fanout registry must admit two leases");
    let mut second = storage::Lease::new();

    {
        let mut first = storage::Lease::new();
        book.try_keep(&mut first, 1).expect("fanout registration");
        book.try_keep(&mut second, 2).expect("fanout registration");
        book.publish();
    }
    assert!(matches!(book.step(), Some(storage::Step::Item(2))));
    assert!(book.step().is_none());
}

#[test]
fn rejects_foreign_and_full() {
    let source = (storage::Registry::try_new(1)).expect("the source fanout must admit one lease");
    let dst =
        (storage::Registry::try_new(0)).expect("the destination fanout must admit zero leases");
    let mut reg = storage::Lease::new();
    let mut empty = storage::Lease::new();

    source.try_keep(&mut reg, 1).expect("fanout registration");
    assert!(matches!(
        dst.try_keep(&mut reg, 0),
        Err(storage::Reject::Active)
    ));
    assert!(matches!(
        dst.try_keep(&mut empty, 0),
        Err(storage::Reject::Full)
    ));
    source.publish();
    assert!(matches!(source.step(), Some(storage::Step::Item(1))));
}

#[test]
fn recycles_broadcast_slot() {
    let book = (storage::Registry::try_new(1)).expect("the fanout probe must admit one lease");

    {
        let mut reg = storage::Lease::new();
        book.try_keep(&mut reg, 1).expect("fanout registration");
        book.publish();
        assert!(matches!(book.step(), Some(storage::Step::Item(1))));
        book.try_keep(&mut reg, 2).expect("fanout registration");
    }

    let mut next = storage::Lease::new();
    book.try_keep(&mut next, 3).expect("fanout registration");
}

#[test]
fn retires_after_updates() {
    let book = (storage::Registry::try_new(1)).expect("book fixture must succeed");
    {
        let mut lease = storage::Lease::new();
        book.try_keep(&mut lease, 1).expect("fanout registration");
        book.try_keep(&mut lease, 2).expect("fanout registration");
        book.publish();
        assert!(matches!(book.step(), Some(storage::Step::Item(2))));
        assert!(book.step().is_none());
        book.try_keep(&mut lease, 3).expect("fanout registration");
        book.publish();
    }
    assert!(!book.has_pending());
    assert!(book.step().is_none());
    let mut next = storage::Lease::new();
    book.try_keep(&mut next, 4).expect("fanout registration");
    book.publish();
    assert!(matches!(book.step(), Some(storage::Step::Item(4))));
    assert!(book.step().is_none());
}
