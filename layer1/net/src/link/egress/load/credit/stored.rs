use std::mem::size_of;

use dope_core::io::transfer;
use o3::mem;

use crate::link::egress::{
    self,
    load::credit,
    metadata::{self, batch},
    reserve::demand,
};

#[must_use = "stored credit must remain attached to an entry or be released"]
pub(in crate::link::egress) struct Stored<'p>(
    mem::raw::Grant<mem::raw::Lane<'p, 'p, 2, credit::Tag, u32>, 2, Amount>,
);

/// One checked partition holding its source grant and immutable payload.
pub(in crate::link::egress) struct Split<'a, 'p, T> {
    grant: mem::raw::Bound<
        &'a mut mem::raw::Grant<mem::raw::Lane<'p, 'p, 2, credit::Tag, u32>, 2, credit::Amount>,
        Parts,
    >,
    load: egress::NonEmpty<T>,
}

struct Parts {
    rest: credit::Amount,
}

pub(in crate::link::egress) struct Part<
    'queue,
    'p,
    'd,
    T,
    V,
    M: credit::Access<'p, 'd>,
    D: demand::Mode,
> {
    claim: credit::Claim<'queue, 'p, 'd, T, M, D>,
    rest: credit::Amount,
    load: egress::NonEmpty<V>,
}

impl<'queue, 'p, 'd, T, V, M: credit::Access<'p, 'd>, D: demand::Mode>
    Part<'queue, 'p, 'd, T, V, M, D>
{
    /// The claim supplies one entry; admission checks its resident charge.
    pub(in crate::link::egress) fn try_new(
        claim: credit::Claim<'queue, 'p, 'd, T, M, D>,
        load: egress::NonEmpty<V>,
    ) -> Result<Self, Rejected<'queue, 'p, 'd, T, V, M, D>> {
        let current = claim.reserve.amount().resident();
        let rest = match current.checked_sub(load.geometry().resident()) {
            Ok(rest) => credit::Amount::new(egress::Entries::new(0), rest),
            Err(cause) => return Err((claim, load, egress::Shortage::Resident(cause))),
        };
        Ok(Self { claim, rest, load })
    }

    pub(in crate::link::egress) fn into_parts(
        self,
    ) -> (credit::Claim<'queue, 'p, 'd, T, M, D>, egress::NonEmpty<V>) {
        (self.claim, self.load)
    }

    pub(in crate::link::egress) fn take(
        self,
    ) -> (
        credit::Reserve<'queue, 'p, 'd, T, M, D>,
        Stored<'p>,
        egress::NonEmpty<V>,
    ) {
        let Self { claim, rest, load } = self;
        let (queue, mut grant) = claim.into_parts();
        let parts = Parts { rest };
        // SAFETY: Part owns the unchanged grant and payload checked by try_new.
        let stored = Stored(unsafe { grant.0.bind_mut(parts) }.split(load.geometry().wire()));
        (credit::Reserve::new(queue, grant), stored, load)
    }
}

impl<'a, 'p, T> Split<'a, 'p, T> {
    pub(in crate::link::egress::load::credit) fn try_new(
        grant: &'a mut credit::Grant<'p>,
        load: egress::NonEmpty<T>,
    ) -> Result<Self, batch::Rejected<T>> {
        let current = grant.amount();
        let held = credit::Amount::new(egress::Entries::new(1), load.geometry().resident());
        let rest = match current.checked_sub(held) {
            Ok(rest) => rest,
            Err(cause) => {
                let reason = egress::Rejection::BatchCapacity {
                    requested: load.charge(),
                    remaining: egress::Credit::new(current.entries(), current.resident()),
                    cause,
                };
                return Err(batch::Rejected { load, reason });
            }
        };
        // SAFETY: subtraction proved this partition of the exclusively borrowed grant.
        let grant = unsafe { grant.0.bind_mut(Parts { rest }) };
        Ok(Self { grant, load })
    }

    pub(in crate::link::egress) fn into_load(self) -> egress::NonEmpty<T> {
        self.load
    }

    pub(in crate::link::egress) fn take(self) -> (Stored<'p>, egress::NonEmpty<T>) {
        let Self { grant, load } = self;
        let stored = Stored(grant.split(load.geometry().wire()));
        (stored, load)
    }
}

// SAFETY: the retained source differs from rest by one entry; bytes never changes credit.
unsafe impl mem::raw::Exact<2, u32, credit::Amount> for Parts {
    type Input = transfer::NonZero;
    type Left = credit::Amount;
    type Right = Amount;

    fn parts(self, source: credit::Amount, bytes: Self::Input) -> (Self::Left, Self::Right) {
        let held = source.sub(self.rest);
        (
            self.rest,
            Amount {
                bytes,
                resident: held.resident(),
            },
        )
    }
}

#[must_use = "bound credit must be published or returned to its queue"]
pub(in crate::link::egress) struct Bound<
    'queue,
    'p,
    'd,
    T,
    M = metadata::Local,
    D: demand::Mode = demand::Fallible,
> where
    M: credit::Access<'p, 'd>,
{
    queue: metadata::Queue<'queue, 'p, 'd, T, M, D>,
    stored: Stored<'p>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(C)]
struct Amount {
    bytes: transfer::NonZero,
    resident: egress::Resident,
}

struct Hold {
    bytes: transfer::NonZero,
}

struct Release;

pub(in crate::link::egress) enum Prefix {
    Whole { remaining: transfer::Len },
    Partial,
}

// SAFETY: Amount statically represents one entry and its exact resident credit.
unsafe impl mem::raw::Amount<2, u32> for Amount {
    fn get(self) -> [u32; 2] {
        [1, self.resident.raw()]
    }
}

// SAFETY: Hold is applied only to a grant proved to contain exactly one entry.
unsafe impl mem::raw::ExactRebind<2, u32, credit::Amount> for Hold {
    type Output = Amount;

    fn rebind(self, source: credit::Amount) -> Amount {
        Amount {
            bytes: self.bytes,
            resident: source.resident(),
        }
    }
}

// SAFETY: only the byte cursor changes; both credit components remain identical.
unsafe impl mem::raw::ExactRebind<2, u32, Amount> for Hold {
    type Output = Amount;

    fn rebind(self, source: Amount) -> Amount {
        Amount {
            bytes: self.bytes,
            resident: source.resident,
        }
    }
}

// SAFETY: every Amount has one entry and keeps its original resident component.
unsafe impl mem::raw::Rebind<2, u32, Amount> for Hold {}

// SAFETY: Release expands Amount's exact one-entry representation without changing it.
unsafe impl mem::raw::ExactRebind<2, u32, Amount> for Release {
    type Output = credit::Amount;

    fn rebind(self, source: Amount) -> credit::Amount {
        credit::Amount::new(egress::Entries::new(1), source.resident)
    }
}

// SAFETY: every Amount expands to the same one-entry resident credit vector.
unsafe impl mem::raw::Rebind<2, u32, Amount> for Release {}

impl<'p> Stored<'p> {
    pub(in crate::link::egress) fn count(&self) -> transfer::NonZero {
        self.0.amount().bytes
    }

    pub(in crate::link::egress) fn consume(&mut self, bytes: transfer::Len) -> Prefix {
        let held = self.0.amount();
        let total = transfer::Len::from_nonzero(held.bytes);
        match total.checked_sub(bytes) {
            Some(rest) => {
                self.0.update(Hold { bytes: rest });
                Prefix::Partial
            }
            None => Prefix::Whole {
                remaining: bytes.abs_diff(total),
            },
        }
    }

    pub(in crate::link::egress) fn into_grant(self) -> credit::Grant<'p> {
        credit::Grant(self.0.map(Release))
    }
}

impl<'queue, 'p, 'd, T, M: credit::Access<'p, 'd>, D: demand::Mode> Bound<'queue, 'p, 'd, T, M, D> {
    pub(in crate::link::egress) fn new(
        bytes: transfer::NonZero,
        claim: credit::Claim<'queue, 'p, 'd, T, M, D>,
    ) -> Self {
        let (queue, grant) = claim.into_parts();
        // SAFETY: Claim is constructed only from an exact one-entry acquisition.
        let stored = Stored(unsafe { grant.0.bind(Hold { bytes }) }.map());
        Self { queue, stored }
    }

    pub(in crate::link::egress) fn into_parts(
        self,
    ) -> (metadata::Queue<'queue, 'p, 'd, T, M, D>, Stored<'p>) {
        (self.queue, self.stored)
    }
}

const _: () = {
    assert!(
        size_of::<Part<'static, 'static, 'static, (), (), metadata::Local, demand::Fallible>>()
            == 7 * size_of::<usize>()
    );
    assert!(size_of::<Stored>() == 3 * size_of::<u64>());
    assert!(size_of::<Bound<'static, 'static, 'static, ()>>() == 5 * size_of::<u64>());
    assert!(
        size_of::<Split<'static, 'static, ()>>()
            == size_of::<&mut credit::Grant<'static>>()
                + size_of::<credit::Amount>()
                + size_of::<egress::NonEmpty<()>>()
    );
};

type Rejected<'queue, 'p, 'd, T, V, M, D> = (
    credit::Claim<'queue, 'p, 'd, T, M, D>,
    egress::NonEmpty<V>,
    egress::Shortage,
);
