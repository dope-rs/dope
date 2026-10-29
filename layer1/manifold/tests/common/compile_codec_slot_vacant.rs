use dope_manifold::client::session::codec::storage::Slot;

fn main() {
    let _ = Slot::new((1u8, ())).run(|owner| {
        let (vacant, value) = owner.take();
        (vacant, value)
    });
}
