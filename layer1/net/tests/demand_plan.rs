pub mod common;

use common::metadata::Owner;
use dope_core::io::transfer;
use dope_net::link::egress::{self, reserve::demand};
use o3::buffer::resident;

#[test]
fn keeps_movable_proof() {
    let load = egress::Load::new(resident::Shared::from(&b"abc"[..]));
    let result = load.try_nonempty();
    assert!(result.is_ok(), "non-empty payload");
    let Ok(body) = result else { return };
    let charge = body.charge();
    let demand = demand::Body::try_new(body.as_inner()).expect("representable body");
    let split = demand::Plan::with_body(demand::Prefix::<2>::FULL, demand)
        .expect("representable split plan was rejected");
    let moved = body;

    assert!(split.wire_len() == transfer::NonZero::fixed::<5>());
    assert!(moved.charge() == charge);
}

#[test]
fn keeps_failures() {
    let owner = Owner {
        resident: usize::MAX,
    };
    let rejected = demand::Body::try_new(&owner);
    assert!(
        matches!(rejected, Err(egress::load::Error::Resident(cause)) if cause.actual().get() == usize::MAX)
    );
}
