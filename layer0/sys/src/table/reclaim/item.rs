/// An affine descriptor transaction that becomes one submitted flight.
pub trait Item: Sized {
    type Slot;
    type Flight;

    fn slot(&self) -> Self::Slot;
    fn submit(self) -> Self::Flight;
}
