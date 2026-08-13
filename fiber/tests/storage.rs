use std::{mem::size_of, pin::pin};

use dope_fiber::{
    abi::Ready,
    task::storage::{Id, RoutedId, RoutedTag, fixed},
};

#[test]
fn routed_ids_reject_cross_route_rebranding_without_layout_growth() {
    struct Owner;
    type Left = RoutedTag<Owner, 7, 3>;
    type Right = RoutedTag<Owner, 7, 4>;

    assert_eq!(size_of::<Id<'static>>(), 8);
    assert_eq!(size_of::<RoutedId<'static, Owner, 7, u8>>(), 16);
    assert_eq!(size_of::<Option<RoutedId<'static, Owner, 7, u8>>>(), 16);

    let mut left = pin!(fixed::Slab::<'static, _, 1, Left>::new());
    let mut right = pin!(fixed::Slab::<'static, _, 1, Right>::new());
    let left_id = left
        .as_mut()
        .insert(Ready::new(()))
        .expect("left task slot");
    let right_id = right
        .as_mut()
        .insert(Ready::new(()))
        .expect("right task slot");
    assert_eq!(left_id.index(), right_id.index());

    let routed = left_id.into_routed(9_u8);
    let routed = match routed.into_typed::<4>() {
        Ok(_) => panic!("a routed id must not be rebranded for another route"),
        Err(routed) => routed,
    };
    assert_eq!(routed.route(), 3);
    assert_eq!(*routed.state(), 9);

    let (left_id, state) = match routed.into_typed::<3>() {
        Ok(typed) => typed,
        Err(_) => panic!("matching route must recover its typed id"),
    };
    assert_eq!(state, 9);
    assert!(left.as_mut().remove(left_id));
    assert!(right.as_mut().remove(right_id));
}
