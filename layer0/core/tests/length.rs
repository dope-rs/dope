use dope_core::io::{fs, fs::metadata};

#[test]
fn preserves_rejected() {
    let observed = fs::Len::try_from(i64::MIN).map_err(metadata::Negative::into_i64);

    assert!(observed == Err(i64::MIN));
}

#[test]
fn seals_valid_length() {
    let observed = fs::Len::try_from(i64::MAX);

    assert!(observed == Ok(fs::Len::fixed::<{ i64::MAX as u64 }>()));
}
