/// One file-table slot whose option chain has a terminal completion.
pub trait Tuning: Copy + Eq {
    type Slot: Copy + Eq;

    fn new(slot: Self::Slot) -> Self;
    fn slot(self) -> Self::Slot;
}
