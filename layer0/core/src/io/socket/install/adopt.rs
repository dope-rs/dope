/// A supplied descriptor does not satisfy the requested socket role.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Error {
    ListenerKind { actual: i32 },
    ListenerState { actual: i32 },
    DatagramKind { actual: i32 },
}
