use dope_core::io::{fs, transfer};
use o3::buffer::storage::Zero;

#[test]
fn reports_overflow() {
    let offset = fs::Offset::fixed::<{ i64::MAX as u64 - 3 }>();
    let length = transfer::Len::fixed::<7>();
    assert!(offset.checked_add(length).is_err());
    assert_eq!(
        fs::Offset::try_new(u64::MAX),
        Err(fs::Overflow {
            actual: u64::MAX,
            maximum: i64::MAX as u64,
        })
    );
    assert_eq!(
        offset.checked_add(transfer::Len::fixed::<3>()),
        Ok(fs::Offset::fixed::<{ i64::MAX as u64 }>())
    );
}

#[test]
fn reports_underflow() {
    let offset = fs::Offset::fixed::<3>();
    assert!(offset.checked_sub(transfer::Len::fixed::<4>()).is_err());
    assert_eq!(
        offset.checked_sub(transfer::Len::fixed::<3>()),
        Ok(fs::Offset::fixed::<0>())
    );
}

#[test]
fn refines_length() {
    assert!(transfer::NonZero::try_from(transfer::Len::ZERO) == Err(Zero));
    let len = transfer::Len::fixed::<7>();
    let nonzero = transfer::NonZero::try_from(len).expect("seven bytes are nonzero");
    assert!(transfer::Len::from_nonzero(nonzero) == len);
}
