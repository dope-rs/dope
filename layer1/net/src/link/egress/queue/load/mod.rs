use o3::buffer::resident::bound;

use crate::link::egress::{
    self, Charge, Excess, NonEmpty, Rejected,
    data::{self, Bound, compose},
    load,
};

pub(in crate::link::egress) mod charge;
pub(in crate::link::egress) mod empty;
pub(in crate::link::egress) mod nonempty;
pub(in crate::link::egress) mod rejected;

/// One immutable payload paired with the exact charge derived before queue
/// mutation. The owned value cannot change between reservation and commit.
#[derive(Debug)]
pub struct Load<B> {
    value: B,
    charge: Charge,
}

impl<B> Load<B> {
    pub fn new<'d>(value: B) -> Self
    where
        B: data::raw::Payload<'d>,
    {
        let charge = Charge::of(&value);
        Self { value, charge }
    }

    pub const fn charge(&self) -> Charge {
        self.charge
    }

    pub const fn as_inner(&self) -> &B {
        &self.value
    }

    pub fn into_inner(self) -> B {
        self.value
    }

    pub fn try_nonempty(self) -> Result<NonEmpty<B>, egress::Rejected<Self, load::Error>> {
        let Self { value, charge } = self;
        let geometry = match charge.try_geometry() {
            Ok(geometry) => geometry,
            Err(reason) => {
                return Err(Rejected::new(Self::from_parts(value, charge), reason));
            }
        };
        Ok(NonEmpty::new(value, geometry.wire(), geometry.resident()))
    }

    /// Checks the actual owner once and retains its exact resident bound.
    pub fn try_bound<G: bound::Bound>(self) -> Result<Load<Bound<B, G>>, Excess<B>> {
        if let bound::Limit::AtMost(maximum) = G::LIMIT
            && self.charge.resident().is_excess(maximum)
        {
            return Err(Excess {
                load: self,
                maximum,
            });
        }
        let (value, charge) = self.into_parts();
        Ok(Load::from_parts(Bound::from_inner(value), charge))
    }

    /// Selects unrestricted retention without a recoverable admission path.
    pub fn unbounded(self) -> Load<Bound<B, bound::Unbounded>> {
        let (value, charge) = self.into_parts();
        Load::from_parts(Bound::from_inner(value), charge)
    }

    /// Wraps the payload in the left branch without changing its exact charge.
    pub fn into_left<R>(self) -> Load<compose::Either<B, R>> {
        let (value, charge) = self.into_parts();
        Load::from_parts(compose::Either::Left(value), charge)
    }

    /// Wraps the payload in the right branch without changing its exact charge.
    pub fn into_right<L>(self) -> Load<compose::Either<L, B>> {
        let (value, charge) = self.into_parts();
        Load::from_parts(compose::Either::Right(value), charge)
    }

    /// Attaches non-payload metadata without measuring the owner again.
    pub fn tagged<K>(self, tag: K) -> Load<compose::Tagged<B, K>> {
        let (value, charge) = self.into_parts();
        Load::from_parts(compose::Tagged::new(value, tag), charge)
    }

    pub(in crate::link) fn into_parts(self) -> (B, Charge) {
        (self.value, self.charge)
    }

    pub(in crate::link) const fn from_parts(value: B, charge: Charge) -> Self {
        Self { value, charge }
    }
}

impl<B, G> Load<Bound<B, G>> {
    /// Injects the proved payload without changing its resident bound or charge.
    pub fn bound_left<R>(self) -> Load<Bound<compose::Either<B, R>, G>> {
        let (value, charge) = self.into_parts();
        Load::from_parts(
            Bound::from_inner(compose::Either::Left(value.into_inner())),
            charge,
        )
    }
}

impl<L, R> Load<compose::Either<L, R>> {
    pub fn split(self) -> compose::Either<Load<L>, Load<R>> {
        let (value, charge) = self.into_parts();
        match value {
            compose::Either::Left(value) => compose::Either::Left(Load::from_parts(value, charge)),
            compose::Either::Right(value) => {
                compose::Either::Right(Load::from_parts(value, charge))
            }
        }
    }
}
