use dope_core::driver::{route, route::table, settings};

#[test]
fn slot_domains() {
    let capacity = settings::FileSlots::fixed::<0, 2>().outbound();

    assert!(matches!(capacity.slot(1), Ok(slot) if capacity.contains(slot)));
    assert!(matches!(
        capacity.slot(2),
        Err(table::bounds::Slot::Capacity(_))
    ));

    let outside = route::SLOT_MASK as usize + 1;
    assert!(matches!(
        capacity.slot(outside),
        Err(table::bounds::Slot::Abi(_))
    ));
}
