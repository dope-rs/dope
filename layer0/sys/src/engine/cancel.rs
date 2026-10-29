use crate::io::reactor::Full;

/// Cancels the option chain owned by a live asynchronous tuning permit.
pub trait Cancel<S> {
    type Permit;

    fn cancel(&mut self, permit: &Self::Permit, slot: S) -> Result<(), Full>;
}
