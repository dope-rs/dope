use o3::{
    base::cell::region,
    depot::{self, index},
    mem::{self, slot},
};

/// An exclusive source loan whose edits remain inside the ingress owner.
/// Codec implementations lend their source without exposing mutable access.
pub struct Edit<'a, S>(pub(in crate::client::session::codec) &'a mut S);

impl<'a, S> Edit<'a, S> {
    pub fn new(source: &'a mut S) -> Self {
        Self(source)
    }
}

/// Owns the initialized storage and controls of one codec.
/// A scoped operation releases its loan only after returning the original
/// occupied owner; a vacant slot cannot complete that operation normally.
#[repr(transparent)]
pub struct Slot<S, C> {
    value: slot::Storage<(S, C)>,
}

impl<S, C> Slot<S, C> {
    pub const fn new(value: (S, C)) -> Self {
        Self {
            value: slot::Storage::filled(value, ()),
        }
    }

    pub fn parts(&self) -> (&S, &C) {
        // SAFETY: construction fills this exact slot, and with retains its initialized owner until return.
        let live: depot::raw::Proof<_, index::Initialized<(S, C)>> =
            unsafe { depot::raw::Proof::new((&self.value, ())) };
        let (storage, state) = live.into();
        (storage, state)
    }

    pub fn parts_mut(&mut self) -> (&mut S, &mut C) {
        let (storage, state) = self.occupied().into_mut();
        (storage, state)
    }

    /// Returns the owned storage and controls, leaving their slot empty.
    pub fn into_parts(mut self) -> (S, C) {
        self.occupied().take().0
    }

    /// Runs one owned operation and returns its exact initialized slot.
    pub fn run<R>(
        mut self,
        run: impl for<'slot, 'id> FnOnce(Owner<'slot, 'id, S, C>) -> (Owner<'slot, 'id, S, C>, R),
    ) -> (Self, R) {
        let output = self.with(run);
        (self, output)
    }

    pub(in crate::client::session) fn with<R>(
        &mut self,
        run: impl for<'slot, 'id> FnOnce(Owner<'slot, 'id, S, C>) -> (Owner<'slot, 'id, S, C>, R),
    ) -> R {
        region::Token::scope(|token| {
            let input = Owner {
                slot: self.occupied(),
                proof: token.proof(),
            };
            run(input).1
        })
    }

    /// The exclusive loan retains this exact slot, initialized by construction or by its scoped return.
    fn occupied(&mut self) -> mem::raw::Occupied<'_, (S, C)> {
        // SAFETY: a consuming transition restores this original owner before releasing its loan.
        let live: depot::raw::Proof<_, index::Initialized<(S, C)>> =
            unsafe { depot::raw::Proof::new((&mut self.value, ())) };
        live.into()
    }
}

/// Exclusive transition authority over one actual initialized codec slot.
pub struct Owner<'slot, 'id, S, C> {
    slot: mem::raw::Occupied<'slot, (S, C)>,
    proof: region::Proof<region::Identity<'id>>,
}

impl<'slot, 'id, S, C> Owner<'slot, 'id, S, C> {
    pub fn parts(&mut self) -> (&mut S, &mut C) {
        let (storage, state) = self.slot.get_mut();
        (storage, state)
    }

    pub fn take(self) -> (Vacant<'slot, 'id, S, C>, (S, C)) {
        let (value, slot) = self.slot.take();
        (
            Vacant {
                slot,
                proof: self.proof,
            },
            value,
        )
    }
}

/// The original codec slot after its storage and controls were consumed.
pub struct Vacant<'slot, 'id, S, C> {
    slot: mem::raw::Vacant<'slot, (S, C)>,
    proof: region::Proof<region::Identity<'id>>,
}

impl<'slot, 'id, S, C> Vacant<'slot, 'id, S, C> {
    pub fn insert(self, value: (S, C)) -> Owner<'slot, 'id, S, C> {
        Owner {
            slot: self.slot.insert(value),
            proof: self.proof,
        }
    }
}
