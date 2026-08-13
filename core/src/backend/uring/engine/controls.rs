use crate::driver::route;

const TUNING_KIND: u8 = 13;
const CLOSE_PREP: u8 = 16;
const CLOSE_KIND: u8 = 17;
const RETIRE_KIND: u8 = 18;

/// Slot identity is sufficient: `Pending` prevents descriptor reuse while the
/// corresponding tuning state remains occupied through terminal settlement.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(in crate::backend::uring) struct Tuning(route::SlotIndex);

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(in crate::backend::uring) struct Close(route::SlotIndex);

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(in crate::backend::uring) struct Retire(route::SlotIndex);

const _: () = assert!(std::mem::size_of::<Tuning>() == std::mem::size_of::<route::SlotIndex>());
const _: () = assert!(std::mem::size_of::<Close>() == std::mem::size_of::<route::SlotIndex>());
const _: () = assert!(std::mem::size_of::<Retire>() == std::mem::size_of::<route::SlotIndex>());

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::backend::uring) enum Decoded {
    Tuning(Tuning),
    ClosePrep,
    Close(Close),
    Retire(Retire),
}

impl TryFrom<route::Token> for Decoded {
    type Error = ();

    fn try_from(token: route::Token) -> Result<Self, Self::Error> {
        if token.route() != route::FRAMEWORK {
            return Err(());
        }
        match (token.kind(), token.epoch_raw()) {
            (TUNING_KIND, 0) => Ok(Self::Tuning(Tuning::new(token.slot()))),
            (CLOSE_PREP, 0) => Ok(Self::ClosePrep),
            (CLOSE_KIND, 0) => Ok(Self::Close(Close::new(token.slot()))),
            (RETIRE_KIND, 0) => Ok(Self::Retire(Retire::new(token.slot()))),
            _ => Err(()),
        }
    }
}

impl Tuning {
    pub(in crate::backend::uring::engine) const fn new(slot: route::SlotIndex) -> Self {
        Self(slot)
    }

    pub(in crate::backend::uring) const fn slot(self) -> route::SlotIndex {
        self.0
    }

    pub(in crate::backend::uring) const fn token(self) -> route::Token {
        route::Token::framework(self.0).with_kind(TUNING_KIND)
    }
}

impl Close {
    pub(in crate::backend::uring) const fn new(slot: route::SlotIndex) -> Self {
        Self(slot)
    }

    pub(in crate::backend::uring) const fn slot(self) -> route::SlotIndex {
        self.0
    }

    pub(in crate::backend::uring) const fn prepare(self) -> route::Token {
        route::Token::framework(self.0).with_kind(CLOSE_PREP)
    }

    pub(in crate::backend::uring) const fn token(self) -> route::Token {
        route::Token::framework(self.0).with_kind(CLOSE_KIND)
    }
}

impl Retire {
    pub(in crate::backend::uring) const fn new(slot: route::SlotIndex) -> Self {
        Self(slot)
    }

    pub(in crate::backend::uring) const fn slot(self) -> route::SlotIndex {
        self.0
    }

    pub(in crate::backend::uring) const fn token(self) -> route::Token {
        route::Token::framework(self.0).with_kind(RETIRE_KIND)
    }
}
