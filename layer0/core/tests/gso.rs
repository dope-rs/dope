use std::num::NonZero;

use dope_core::io::datagram::{GSO_LIMITS, Gso, Shape};
use o3::buffer::bytes::bounded::Value;

#[test]
fn payload_length() {
    let Some(limits) = GSO_LIMITS else { return };
    let size = NonZero::<u16>::new(1_000).expect("non-zero segment size became zero");
    let bytes = vec![0; 2_000];
    let pointer = bytes.as_ptr();
    let payload = Value::try_new(bytes).ok().expect("bounded payload");
    let batch = limits.payload(payload, size).expect("valid GSO fixture");

    let payload = Gso::acquire().expect("GSO capability").payload(batch);
    assert_eq!(payload.into_payload().as_ref().as_ptr(), pointer);
    let empty = Value::try_new(Vec::<u8>::new())
        .ok()
        .expect("bounded empty payload");
    assert!(matches!(
        limits.payload(empty, size).err().map(|error| error.into_parts().1),
        Some(Shape::Segments {
            actual: 0,
            minimum: 2,
            maximum,
        }) if maximum == limits.capacity().count()
    ));
}
