use dope_manifold::client::session::codec::storage::Slot;

fn main() {
    let (slot, ()) = Slot::new((vec![1u8], 2u8)).run(|mut owner| {
        owner.parts().0[0] = 3;
        let (vacant, value) = owner.take();
        (vacant.insert(value), ())
    });
    assert_eq!(slot.into_parts(), (vec![3], 2));
}
