use crate::table::token::host::Closed;

/// Binds kernel identities to the host's original affine completion owners.
pub trait Drain {
    type Key;
    type Returned;
    type Flight;
    type Echo;
    type Class;
    type Driver;

    fn driver(&self) -> Self::Driver;
    fn bind(&self, key: Self::Key) -> Option<Self::Flight>;
    fn retired(&self, returned: Self::Returned) -> Closed<Self::Echo, Self::Class>;
}
