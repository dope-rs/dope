use dope_core::io::transfer;

use crate::link::egress::{
    Resident,
    load::credit,
    reserve::{Bounds, Entries, bounds::Shape, limit::Limit},
};

/// Retained final-link geometry, including its native index storage.
pub trait Geometry: sealed::Geometry + 'static {}

/// One fixed-capacity prefix with no body reservation.
#[derive(Clone, Copy)]
pub struct Prefix<const BYTES: usize>(());

impl<const BYTES: usize> Prefix<BYTES> {
    pub(in crate::link::egress) const fn new() -> Self {
        const { assert!(BYTES > 0 && BYTES <= transfer::MAX_BYTES) }
        Self(())
    }
}

impl Geometry for Bounds {}
impl<const BYTES: usize> Geometry for Prefix<BYTES> {}

mod sealed {
    use super::{Bounds, Limit, Resident, Shape};

    pub trait Geometry: Copy {
        type Indices<T: Copy>: Copy;
        type Selection: Copy;

        fn bounds(self) -> Bounds;
        fn first(self) -> Limit;
        fn second(self) -> Limit;
        fn resident(self) -> Resident;
        fn shape(self) -> Option<Self::Selection>;
        fn kind(selection: Self::Selection) -> Shape;
        fn store<T: Copy>(shape: Self::Selection, first: T, second: Option<T>) -> Self::Indices<T>;
        fn parts<T: Copy>(indices: Self::Indices<T>) -> (T, Option<T>, bool);
    }
}

impl sealed::Geometry for Bounds {
    type Indices<T: Copy> = (Shape, T, Option<T>);
    type Selection = Shape;

    fn bounds(self) -> Bounds {
        self
    }

    fn first(self) -> Limit {
        self.first()
    }

    fn second(self) -> Limit {
        self.second()
    }

    fn resident(self) -> Resident {
        self.resident()
    }

    fn shape(self) -> Option<Self::Selection> {
        self.shape()
    }

    fn kind(selection: Self::Selection) -> Shape {
        selection
    }

    fn store<T: Copy>(shape: Shape, first: T, second: Option<T>) -> Self::Indices<T> {
        (shape, first, second)
    }

    fn parts<T: Copy>((shape, first, second): Self::Indices<T>) -> (T, Option<T>, bool) {
        (first, second, matches!(shape, Shape::Second))
    }
}

impl<const BYTES: usize> sealed::Geometry for Prefix<BYTES> {
    type Indices<T: Copy> = T;
    type Selection = ();

    fn bounds(self) -> Bounds {
        Bounds::one(self.first())
    }

    fn first(self) -> Limit {
        Limit::bounded::<BYTES>(Resident::new(BYTES as u32))
    }

    fn second(self) -> Limit {
        Limit::EMPTY
    }

    fn resident(self) -> Resident {
        Resident::new(BYTES as u32)
    }

    fn shape(self) -> Option<Self::Selection> {
        Some(())
    }

    fn kind((): Self::Selection) -> Shape {
        Shape::First
    }

    fn store<T: Copy>((): Self::Selection, first: T, _: Option<T>) -> Self::Indices<T> {
        first
    }

    fn parts<T: Copy>(first: Self::Indices<T>) -> (T, Option<T>, bool) {
        (first, None, false)
    }
}

pub(super) fn queued<G: Geometry>(geometry: G, shape: Option<G::Selection>) -> credit::Queued {
    let entries = match shape.map(G::kind) {
        None => Entries::Zero,
        Some(Shape::First | Shape::Second) => Entries::One,
        Some(Shape::Pair) => Entries::Two,
    };
    credit::Queued::new(entries, geometry.resident())
}
