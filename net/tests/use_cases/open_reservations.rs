use std::panic::{AssertUnwindSafe, catch_unwind};

use dope_net::wire::identity::Identity;
use dope_net::wire::reservation::ReservedOpen;
use dope_net::wire::{OpenReservation, OpenRollback, ReadyOpen, Wire};

#[derive(Default)]
struct Rollback {
    count: usize,
}

impl OpenRollback<Identity, ()> for Rollback {
    fn rollback_open(&mut self, open: (Identity, ())) {
        let (Identity, ()) = open;
        self.count += 1;
    }
}

#[test]
fn ready_open_is_exact_committed_storage() {
    type Open = ReadyOpen<Identity, ()>;
    type Committed = (Identity, <Identity as Wire>::SendStorage);

    assert_eq!(size_of::<Open>(), size_of::<Committed>());
    assert_eq!(align_of::<Open>(), align_of::<Committed>());

    let (Identity, ()) = ReadyOpen::new(Identity, ()).commit();
}

#[test]
fn reserved_open_has_only_context_storage_overhead() {
    type Open<'a> = ReservedOpen<'a, Identity, (), Rollback>;

    assert_eq!(size_of::<Open<'_>>(), size_of::<&mut Rollback>());
    assert_eq!(align_of::<Open<'_>>(), align_of::<&mut Rollback>());
}

#[test]
fn reserved_open_commit_skips_rollback() {
    let mut rollback = Rollback::default();

    let open = ReservedOpen::new(&mut rollback, Identity, ());
    let (Identity, ()) = open.commit();

    assert_eq!(rollback.count, 0);
}

#[test]
fn reserved_open_drop_rolls_back() {
    let mut rollback = Rollback::default();

    drop(ReservedOpen::new(&mut rollback, Identity, ()));

    assert_eq!(rollback.count, 1);
}

#[test]
fn reserved_open_unwind_rolls_back() {
    let mut rollback = Rollback::default();

    let result = catch_unwind(AssertUnwindSafe(|| {
        let _open = ReservedOpen::new(&mut rollback, Identity, ());
        panic!("cancel reserved open");
    }));

    assert!(result.is_err());
    assert_eq!(rollback.count, 1);
}
