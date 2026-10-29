use dope_sys::table::token;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct Tuning<S>(S);

impl<S: Copy + Eq> token::Tuning for Tuning<S> {
    type Slot = S;

    fn new(slot: S) -> Self {
        Self(slot)
    }

    fn slot(self) -> S {
        self.0
    }
}
