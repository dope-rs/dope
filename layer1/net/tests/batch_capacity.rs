use dope_net::wire::{
    Identity,
    receive::batch::{Capacity, Spare},
};

#[test]
fn fits_identity_quota() {
    let capacity = Capacity::<'static, Identity>::fit_spare(Spare::from_usize(usize::MAX));

    assert!(matches!(capacity, Some(value) if value.items().get() == 1));
}
