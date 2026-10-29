use dope_test::alloc::Tracker;
use o3::depot::queue::resident;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn table_allocates_once() {
    use dope_core::driver::schedule::ready::wake::table::storage;

    let (created, build) = Tracker::counts(|| storage::Registry::<usize>::try_new(4));
    assert_eq!(build, [1, 0]);
    let table = (created).expect("the measured resident table must allocate once");

    let ((), hot) = Tracker::counts(|| {
        let slot = (table.try_slot(0)).expect("test precondition");
        let mut reg = storage::Registration::new();
        assert_eq!(slot.try_keep(&mut reg, 1), Ok(()));
        assert!(slot.take_one() == resident::Take::Item(1));
        assert_eq!(slot.try_keep(&mut reg, 2), Ok(()));
    });
    assert_eq!(hot, [0, 0]);
    let ((), release) = Tracker::counts(move || assert!(!table.has_waiters()));
    assert_eq!(release, [0, 1]);
}

#[test]
fn fanout_allocates_once() {
    use dope_core::driver::schedule::ready::wake::fanout::storage;

    let (created, build) = Tracker::counts(|| storage::Registry::<usize>::try_new(4));
    assert_eq!(build, [1, 0]);
    let registry = (created).expect("the measured fanout registry must allocate once");

    let ((), hot) = Tracker::counts(|| {
        let mut lease = storage::Lease::new();
        assert!(registry.try_keep(&mut lease, 1) == Ok(()));
        registry.publish();
        assert!(matches!(registry.step(), Some(storage::Step::Item(1))));
        assert!(registry.try_keep(&mut lease, 2) == Ok(()));
        registry.publish();
    });
    assert_eq!(hot, [0, 0]);
    let ((), release) = Tracker::counts(move || assert!(!registry.has_pending()));
    assert_eq!(release, [0, 1]);
}
