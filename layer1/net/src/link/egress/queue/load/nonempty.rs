use dope_core::io::transfer;
use o3::buffer::resident::{self, bound};

use crate::link::egress::{
    Charge, Geometry, Rejected, Resident, ResidentSize, WireBytes,
    data::{self, compose},
    load::{self, lease},
};

/// An immutable load proven to carry at least one logical byte.
#[derive(Debug)]
pub struct NonEmpty<B> {
    value: B,
    len: transfer::NonZero,
    resident: Resident,
}

impl<B> NonEmpty<B> {
    /// Validates a newly constructed payload, consuming it on failure.
    pub fn try_new<'d>(value: B) -> Result<Self, load::Error>
    where
        B: data::raw::Payload<'d>,
    {
        let geometry = Charge::of(&value).try_geometry()?;
        Ok(Self::new(value, geometry.wire(), geometry.resident()))
    }

    pub(in crate::link::egress) const fn new(
        value: B,
        len: transfer::NonZero,
        res: Resident,
    ) -> Self {
        Self {
            value,
            len,
            resident: res,
        }
    }

    pub fn charge(&self) -> Charge {
        Charge::new(
            WireBytes::from(transfer::Len::from_nonzero(self.len)),
            ResidentSize::from(self.resident),
        )
    }

    pub const fn geometry(&self) -> Geometry {
        Geometry::new(self.len, self.resident)
    }

    pub const fn as_inner(&self) -> &B {
        &self.value
    }

    pub fn into_inner(self) -> B {
        self.value
    }

    /// Checks the resident bound while retaining the exact nonempty geometry.
    pub fn try_bound<G: bound::Bound>(
        self,
    ) -> Result<NonEmpty<data::Bound<B, G>>, Rejected<Self, ResidentSize>> {
        if let bound::Limit::AtMost(maximum) = G::LIMIT
            && ResidentSize::from(self.resident).is_excess(maximum)
        {
            return Err(Rejected::new(self, ResidentSize::new(maximum)));
        }
        Ok(NonEmpty::new(
            data::Bound::from_inner(self.value),
            self.len,
            self.resident,
        ))
    }

    /// Retains the exact payload under its original owner's rollback contract.
    pub fn lease<R: lease::Commit<Item = B>>(self, owner: R) -> NonEmpty<lease::Lease<R>> {
        NonEmpty::new(
            lease::Lease::new(owner, self.value),
            self.len,
            self.resident,
        )
    }

    pub fn into_left<R>(self) -> NonEmpty<compose::Either<B, R>> {
        NonEmpty::new(compose::Either::Left(self.value), self.len, self.resident)
    }

    pub fn into_right<L>(self) -> NonEmpty<compose::Either<L, B>> {
        NonEmpty::new(compose::Either::Right(self.value), self.len, self.resident)
    }
}

impl<'d> NonEmpty<resident::Shared<'d>> {
    pub fn from_array<const N: usize>(bytes: &'d [u8; N]) -> Self {
        Self::new(
            resident::Shared::from(bytes.as_slice()),
            transfer::NonZero::fixed_size::<N>(),
            Resident::ZERO,
        )
    }
}

impl<L, R> NonEmpty<compose::Either<L, R>> {
    pub fn split(self) -> compose::Either<NonEmpty<L>, NonEmpty<R>> {
        let Self {
            value,
            len,
            resident,
        } = self;
        match value {
            compose::Either::Left(value) => {
                compose::Either::Left(NonEmpty::new(value, len, resident))
            }
            compose::Either::Right(value) => {
                compose::Either::Right(NonEmpty::new(value, len, resident))
            }
        }
    }
}

impl<B, G> NonEmpty<data::Bound<B, G>> {
    /// Injects the proved payload without changing its resident bound or geometry.
    pub fn bound_right<L>(self) -> NonEmpty<data::Bound<compose::Either<L, B>, G>> {
        NonEmpty::new(
            data::Bound::from_inner(compose::Either::Right(self.value.into_inner())),
            self.len,
            self.resident,
        )
    }

    /// Removes the resident bound without remeasuring the unchanged payload.
    pub fn into_unbound(self) -> NonEmpty<B> {
        NonEmpty::new(self.value.into_inner(), self.len, self.resident)
    }
}

impl<T, K> NonEmpty<compose::Tagged<T, K>> {
    pub(in crate::link::egress) fn untag(self) -> NonEmpty<T> {
        NonEmpty::new(self.value.into_parts().0, self.len, self.resident)
    }
}

impl<R: lease::Commit> NonEmpty<lease::Lease<R>> {
    pub(in crate::link::egress) fn settle(self) -> (R, NonEmpty<R::Item>) {
        let Self {
            value,
            len,
            resident,
        } = self;
        let (owner, value) = value.into_parts();
        (owner, NonEmpty::new(value, len, resident))
    }
}
