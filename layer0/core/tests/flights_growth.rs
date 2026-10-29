use dope_core::driver::retained::flight::storage::Flights;
use o3::depot::arena::slab::Vacancy;

#[test]
fn keeps_exact_capacity() {
    let mut arena = Flights::<(), 3, 32>::new();
    let mut builder = arena.builder();
    for capacity in 0..=4 {
        let [slots] = builder
            .try_batch([capacity])
            .expect("released backing must fit growing demand in eight native indices")
            .groups();
        let held: Vec<_> = (0..capacity)
            .map(|_| slots.try_vacancy().expect("requested vacancy").reserve(()))
            .collect();
        assert!(
            matches!(slots.try_vacancy(), Err(Vacancy::Full)),
            "rounded backing stays unissued"
        );
        drop(held);
    }
}

#[test]
fn preserves_admission() {
    let mut arena = Flights::<(), 3, 32>::new();
    drop(
        arena
            .builder()
            .try_batch([5, 3])
            .expect("optional rounding must preserve room for the exact batch")
            .groups(),
    );
}
