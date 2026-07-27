use dope_net::wire::identity::Identity;
use dope_net::wire::{OpenReservation, ReadyOpen, Wire};

#[test]
fn ready_open_is_exact_committed_storage() {
    type Open = ReadyOpen<Identity>;
    type Committed = (Identity, <Identity as Wire>::SendStorage);

    assert_eq!(size_of::<Open>(), size_of::<Committed>());
    assert_eq!(align_of::<Open>(), align_of::<Committed>());

    let (Identity, ()) = ReadyOpen::new(Identity, ()).commit();
}
