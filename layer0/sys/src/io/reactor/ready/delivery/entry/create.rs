use crate::io::socket;

/// Constructs a native event with its exact outbound descriptor ownership.
pub trait Create<O, H> {
    type Key;
    type Output;
    fn created(key: Self::Key, slot: O, result: Result<H, socket::Create>) -> Self::Output;
}
