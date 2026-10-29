use std::mem;

use dope_core::driver::schedule::ready::wake::{
    fanout, table,
    table::storage::{Registration, Registry},
};
use o3::depot::queue::resident;

#[test]
fn keeps_compact_layouts() {
    let word = mem::size_of::<usize>();
    assert_eq!(
        [
            mem::size_of::<table::storage::Registration<'static, u64>>(),
            mem::size_of::<fanout::storage::Lease<'static, u64>>(),
        ],
        [2 * word; 2],
    );
    assert_eq!(
        [
            mem::size_of::<Option<table::storage::Registration<'static, u64>>>(),
            mem::size_of::<Option<fanout::storage::Lease<'static, u64>>>(),
        ],
        [3 * word; 2],
    );
    assert_eq!(
        [
            mem::size_of::<table::storage::Registry<u64>>(),
            mem::size_of::<fanout::storage::Registry<u64>>(),
        ],
        [4 * word, 6 * word],
    );
}

#[test]
fn tracks_exact_table() {
    use dope_core::driver::schedule::ready::wake::table::storage;

    let result = storage::Registry::try_new(3);
    let book = (result).expect("the exact table must admit three slots");
    let mut second = storage::Registration::new();
    let mut used = storage::Registration::new();
    let slot0 = (book.try_slot(0)).expect("the table must expose its first slot");
    let slot2 = (book.try_slot(2)).expect("the table must expose its third slot");

    {
        let mut first = storage::Registration::new();
        assert_eq!(slot2.try_keep(&mut first, 20), Ok(()));
        assert_eq!(slot0.try_keep(&mut second, 10), Ok(()));
        assert_eq!(slot2.try_keep(&mut first, 21), Ok(()));
        assert_eq!(
            slot0.try_keep(&mut first, 0),
            Err(storage::Reject::Active { index: 0 })
        );
        assert!(matches!(
            slot2.try_keep(&mut used, 0),
            Err(storage::Reject::Occupied { index: 2 })
        ));
        assert!(slot2.take_one() == resident::Take::Item(21));
        assert_eq!(slot2.try_keep(&mut first, 22), Ok(()));
        assert!(slot0.take_one() == resident::Take::Item(10));
        assert!(slot2.take_one() == resident::Take::Item(22));
        assert_eq!(slot2.try_keep(&mut first, 23), Ok(()));
    }
    assert!(!book.has_waiters());
    assert_eq!(slot2.try_keep(&mut used, 23), Ok(()));
}

#[test]
fn releases_parked_table() {
    let book = Registry::try_new(1).expect("one table slot must fit");
    let slot = book.try_slot(0).expect("the table must expose its slot");
    {
        let mut reg = Registration::new();
        assert_eq!(slot.try_keep(&mut reg, 1), Ok(()));
        assert!(slot.take_one() == resident::Take::Item(1));
        assert!(!book.has_waiters());
    }
    let mut reg = Registration::new();
    assert_eq!(slot.try_keep(&mut reg, 2), Ok(()));
    assert!(slot.take_one() == resident::Take::Item(2));
    assert!(slot.take_one() == resident::Take::Empty);
}

#[test]
fn rejects_foreign_table() {
    use dope_core::driver::schedule::ready::wake::table::storage;

    let result = storage::Registry::try_new(2);
    let source = (result).expect("the source table must admit two slots");
    let result = storage::Registry::try_new(2);
    let dst = (result).expect("the target table must admit two slots");
    let mut used = storage::Registration::new();
    let src0 = (source.try_slot(0)).expect("the source table must expose its first slot");
    let dst0 = (dst.try_slot(0)).expect("the target table must expose its first slot");
    let dst1 = (dst.try_slot(1)).expect("the target table must expose its second slot");

    {
        let mut reg = storage::Registration::new();
        assert_eq!(src0.try_keep(&mut reg, 1), Ok(()));
        assert_eq!(dst1.try_keep(&mut used, 9), Ok(()));
        assert!(matches!(
            dst1.try_keep(&mut reg, 0),
            Err(storage::Reject::Active { index: 1 })
        ));
        assert!(src0.take_one() == resident::Take::Item(1));
    }

    {
        let mut reg = storage::Registration::new();
        assert_eq!(src0.try_keep(&mut reg, 3), Ok(()));
        assert!(matches!(
            dst0.try_keep(&mut reg, 0),
            Err(storage::Reject::Active { index: 0 })
        ));
        assert!(src0.take_one() == resident::Take::Item(3));
    }
    let mut reg = storage::Registration::new();
    assert_eq!(dst0.try_keep(&mut reg, 4), Ok(()));
    assert!(dst0.take_one() == resident::Take::Item(4));
}

#[test]
fn reports_exact_bounds() {
    let table = Registry::<u64>::try_new(2);
    let table = (table).expect("the table must admit two slots");
    assert!(matches!(
        table.try_slot(2),
        Err(bounds) if bounds.index == 2 && bounds.len == 2
    ));
}
