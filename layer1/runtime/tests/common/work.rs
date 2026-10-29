use dope_runtime::executor::work;

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum Slot {
    Zero,
    One,
    Two,
    Three,
}

impl work::Cycle for Slot {
    const FIRST: Self = Self::Zero;

    fn next(self) -> Self {
        match self {
            Self::Zero => Self::One,
            Self::One => Self::Two,
            Self::Two => Self::Three,
            Self::Three => Self::Zero,
        }
    }
}
