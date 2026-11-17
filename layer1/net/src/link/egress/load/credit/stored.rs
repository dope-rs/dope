use std::mem::size_of;

use dope_core::io::transfer;

use crate::link::egress::{
    self,
    data::position,
    load::credit,
    metadata::{self, batch},
    reserve::demand,
};

#[must_use = "stored credit must remain attached to an entry or be released"]
pub(in crate::link::egress) struct Stored<'p, 'd> {
    account: credit::Account<'p, 'd>,
    bytes: transfer::NonZero,
    resident: egress::Resident,
}

pub(in crate::link::egress) struct Split<
    'a,
    'queue,
    'p,
    'd,
    T,
    M: credit::Access<'p, 'd>,
    D: demand::Mode,
    P: position::State,
> {
    reserve: &'a mut credit::Reserve<'queue, 'p, 'd, T, M, D, P>,
    rest: credit::Amount,
    load: egress::NonEmpty<T>,
}

pub(in crate::link::egress) struct Part<
    'queue,
    'p,
    'd,
    T,
    V,
    M: credit::Access<'p, 'd>,
    D: demand::Mode,
    P: position::State = (),
> {
    claim: credit::Claim<'queue, 'p, 'd, T, M, D, P>,
    rest: egress::Resident,
    load: egress::NonEmpty<V>,
}

impl<'queue, 'p, 'd, T, V, M: credit::Access<'p, 'd>, D: demand::Mode, P: position::State>
    Part<'queue, 'p, 'd, T, V, M, D, P>
{
    pub(in crate::link::egress) fn try_new(
        claim: credit::Claim<'queue, 'p, 'd, T, M, D, P>,
        load: egress::NonEmpty<V>,
    ) -> Result<Self, Rejected<'queue, 'p, 'd, T, V, M, D, P>> {
        let rest = match claim.resident.checked_sub(load.geometry().resident()) {
            Ok(rest) => rest,
            Err(cause) => return Err((claim, load, egress::Shortage::Resident(cause))),
        };
        Ok(Self { claim, rest, load })
    }

    pub(in crate::link::egress) fn into_parts(
        self,
    ) -> (
        credit::Claim<'queue, 'p, 'd, T, M, D, P>,
        egress::NonEmpty<V>,
    ) {
        (self.claim, self.load)
    }

    pub(in crate::link::egress) fn take(
        self,
    ) -> (
        credit::Reserve<'queue, 'p, 'd, T, M, D, P>,
        Stored<'p, 'd>,
        egress::NonEmpty<V>,
    ) {
        let Self { claim, rest, load } = self;
        let stored = Stored {
            account: claim.account,
            bytes: load.geometry().wire(),
            resident: load.geometry().resident(),
        };
        let rest = credit::Reserve::new(
            claim.queue,
            claim.account,
            credit::Amount::new(egress::Entries::new(0), rest),
        );
        (rest, stored, load)
    }
}

impl<'a, 'queue, 'p, 'd, T, M: credit::Access<'p, 'd>, D: demand::Mode, P: position::State>
    Split<'a, 'queue, 'p, 'd, T, M, D, P>
{
    pub(super) fn try_new(
        reserve: &'a mut credit::Reserve<'queue, 'p, 'd, T, M, D, P>,
        load: egress::NonEmpty<T>,
    ) -> Result<Self, batch::Rejected<T>> {
        let current = reserve.amount;
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
        Ok(Self {
            reserve,
            rest,
            load,
        })
    }

    pub(in crate::link::egress) fn into_load(self) -> egress::NonEmpty<T> {
        self.load
    }

    pub(in crate::link::egress) fn take(self) -> (Stored<'p, 'd>, egress::NonEmpty<T>) {
        let Self { reserve, rest, load } = self;
        reserve.amount = rest;
        let stored = Stored {
            account: reserve.account,
            bytes: load.geometry().wire(),
            resident: load.geometry().resident(),
        };
        (stored, load)
    }
}

pub(in crate::link::egress) enum Prefix {
    Whole { remaining: transfer::Len },
    Partial,
}

impl Stored<'_, '_> {
    pub(in crate::link::egress) fn count(&self) -> transfer::NonZero {
        self.bytes
    }

    pub(in crate::link::egress) fn consume(&mut self, bytes: transfer::Len) -> Prefix {
        let total = transfer::Len::from_nonzero(self.bytes);
        match total.checked_sub(bytes) {
            Some(rest) => {
                self.bytes = rest;
                Prefix::Partial
            }
            None => Prefix::Whole {
                remaining: bytes.abs_diff(total),
            },
        }
    }

    pub(in crate::link::egress) fn release(self) {
        // SAFETY: this entry consumes its exact one-entry acquisition.
        unsafe {
            self.account
                .release(credit::Amount::new(egress::Entries::new(1), self.resident));
        }
    }
}

impl<'queue, 'p, 'd, T, M: credit::Access<'p, 'd>, D: demand::Mode, P: position::State>
    credit::Claim<'queue, 'p, 'd, T, M, D, P>
{
    pub(in crate::link::egress) fn bind(
        self,
        bytes: transfer::NonZero,
    ) -> (metadata::Queue<'queue, 'p, 'd, T, M, D, P>, Stored<'p, 'd>) {
        let stored = Stored {
            account: self.account,
            bytes,
            resident: self.resident,
        };
        (self.queue, stored)
    }
}

const _: () = {
    assert!(
        size_of::<Part<'static, 'static, 'static, (), (), metadata::Local, demand::Fallible>>()
            == 7 * size_of::<usize>()
    );
    assert!(size_of::<Stored<'static, 'static>>() == 3 * size_of::<u64>());
    assert!(
        size_of::<Split<
            'static, 'static, 'static, 'static, (), metadata::Local, demand::Fallible, (),
        >>() == size_of::<&mut credit::Reserve<'static, 'static, 'static, ()>>()
            + size_of::<credit::Amount>()
            + size_of::<egress::NonEmpty<()>>()
    );
};

type Rejected<'queue, 'p, 'd, T, V, M, D, P> = (
    credit::Claim<'queue, 'p, 'd, T, M, D, P>,
    egress::NonEmpty<V>,
    egress::Shortage,
);
