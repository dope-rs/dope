mod boundary;
pub(crate) use boundary::Boundary;

pub mod data;
pub mod life;
pub mod set;
pub mod wire;
pub mod writable;

/// One runtime-issued event with its associated arguments and phase.
#[repr(transparent)]
pub struct Event<T, const PHASE: u8> {
    parts: T,
}

impl<T, const PHASE: u8> Event<T, PHASE> {
    pub(in crate::runtime) const fn new(parts: T) -> Self {
        Self { parts }
    }

    pub fn into_parts(self) -> T {
        self.parts
    }
}

impl<A, B, C, D, E, const PHASE: u8> From<Event<(A, B, C, D, E), PHASE>> for (A, B, C, D, E) {
    fn from(event: Event<(A, B, C, D, E), PHASE>) -> Self {
        event.into_parts()
    }
}
