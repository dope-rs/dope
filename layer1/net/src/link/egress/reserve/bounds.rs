use crate::link::egress::{
    self, Resident,
    reserve::{demand, error, limit},
};

#[derive(Clone, Copy)]
pub enum Shape {
    First,
    Second,
    Pair,
}

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u32)]
pub(in crate::link::egress) enum Entries {
    Zero,
    One,
    Two,
}

impl Entries {
    pub(in crate::link::egress) const fn into_credit(self) -> egress::Entries {
        egress::Entries::new(self as u32)
    }
}

/// Bounds for at most two ordered final-link entries.
#[derive(Clone, Copy, Debug)]
pub struct Bounds {
    first: limit::Limit,
    second: limit::Limit,
    resident: Resident,
}

impl Bounds {
    pub(in crate::link::egress) const fn proven<const PREFIX: usize>(
        capacity: demand::Capacity<PREFIX>,
    ) -> Self {
        Self {
            first: limit::Limit::bounded::<PREFIX>(Resident::new(PREFIX as u32)),
            second: limit::Limit::open(Resident::new(capacity.body())),
            resident: Resident::new(PREFIX as u32 + capacity.body()),
        }
    }

    pub const fn one(first: limit::Limit) -> Self {
        let resident = first.resident();
        Self {
            first,
            second: limit::Limit::EMPTY,
            resident,
        }
    }

    pub fn pair(first: limit::Limit, second: limit::Limit) -> Result<Self, error::Pair> {
        let resident = first
            .resident()
            .checked_add(second.resident())
            .map_err(error::Pair::new)?;
        Ok(Self {
            first,
            second,
            resident,
        })
    }

    pub(super) const fn first(self) -> limit::Limit {
        self.first
    }

    pub(super) const fn second(self) -> limit::Limit {
        self.second
    }

    pub(super) const fn shape(self) -> Option<Shape> {
        match (self.first.is_active(), self.second.is_active()) {
            (true, false) => Some(Shape::First),
            (false, true) => Some(Shape::Second),
            (true, true) => Some(Shape::Pair),
            (false, false) => None,
        }
    }

    pub(in crate::link::egress) const fn entries(self) -> Entries {
        match (self.first.is_active(), self.second.is_active()) {
            (false, false) => Entries::Zero,
            (true, false) | (false, true) => Entries::One,
            (true, true) => Entries::Two,
        }
    }

    pub(in crate::link::egress) const fn resident(self) -> Resident {
        self.resident
    }
}

const _: () = {
    assert!(std::mem::size_of::<Entries>() == std::mem::size_of::<u32>());
    assert!(std::mem::size_of::<Bounds>() == 7 * std::mem::size_of::<u32>());
};
