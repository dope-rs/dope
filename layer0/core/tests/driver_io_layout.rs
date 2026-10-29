use std::mem::size_of;

#[test]
fn receive_stale_is_zst() {
    assert_eq!(size_of::<dope_core::driver::receive::Stale>(), 0);
}
