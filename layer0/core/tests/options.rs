use std::time::Duration;

use dope_core::io::socket::option;

#[test]
fn stream_buffer_abi_bounds() {
    assert!(matches!(
        option::Buffer::try_from(usize::MAX),
        Err(option::Error::Buffer {
            actual: usize::MAX,
            maximum,
        }) if maximum == i32::MAX as usize
    ));
}

#[test]
fn keep_alive_bounds() {
    assert!(matches!(
        option::Period::try_from(Duration::ZERO),
        Err(option::Error::Period {
            actual: Duration::ZERO,
            maximum,
        }) if maximum == Duration::from_secs(i32::MAX as u64)
    ));
    assert!(matches!(
        option::Retries::try_from(0),
        Err(option::Error::Retries {
            actual: 0,
            maximum,
        }) if maximum == i32::MAX as u32
    ));
    assert!(matches!(
        option::Period::try_from(Duration::MAX),
        Err(option::Error::Period {
            actual: Duration::MAX,
            maximum,
        }) if maximum == Duration::from_secs(i32::MAX as u64)
    ));
}
