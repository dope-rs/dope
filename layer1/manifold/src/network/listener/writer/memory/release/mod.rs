use std::{mem::size_of, pin};

pub(in crate::network::listener) use claim::Claim;
use dope_core::driver::{
    route::{Identity, KeyTag},
    schedule::ready,
};
use dope_net::link::pool;
pub(in crate::network::listener) use gate::Gate;
use o3::{
    depot::{
        self,
        arena::pool::{Unique, recycle},
        queue::indexed::fifo::staged,
    },
    mem::{self, Return, lease::Open},
};

use crate::network::listener::writer::memory;

mod claim;
mod gate;

pub(in crate::network::listener) struct Source<'p, 's, 'd, T, ID: Identity> {
    gate: &'p Gate<'s, 'd, T, ID>,
    proof: staged::Member<
        'p,
        ready::Retained<'d>,
        (),
        Unique<'s, T, recycle::Header<T>>,
        pool::raw::Physical<'d, ID>,
    >,
}

pub(in crate::network::listener) struct Lease<'p, 's, 'd, T, ID: Identity> {
    lease: mem::Lease<Wake<'p, 's, 'd, T, ID>, Open>,
}

struct Wake<'p, 's, 'd, T, ID: Identity>(&'p Gate<'s, 'd, T, ID>);

impl<'p, 's, 'd, T, ID: Identity> Source<'p, 's, 'd, T, ID> {
    pub(in crate::network::listener::writer::memory) fn from(
        gate: &'p Gate<'s, 'd, T, ID>,
        lane: pool::raw::Physical<'d, ID>,
    ) -> Option<Self> {
        let proof = staged::Member::get(&gate.permits, lane)?;
        Some(Self { gate, proof })
    }

    pub(in crate::network::listener::writer::memory) fn claim(
        self,
        target: ready::Target<'d>,
    ) -> Claim<Lease<'p, 's, 'd, T, ID>> {
        let Self { gate, proof } = self;
        let key = target.key();
        let queued = proof.has_queued();
        // SAFETY: this synchronous claim exclusively consumes the exact retained slot.
        match staged::Entry::from_source(unsafe { depot::raw::Proof::new(proof) }) {
            staged::Entry::Vacant(entry) if queued => {
                entry.insert(key, ());
                Claim::Waiting
            }
            staged::Entry::Vacant(entry) => match gate.store.take() {
                Some(lease) => Claim::Acquired(Lease::new(lease, gate)),
                None => {
                    entry.insert(key, ());
                    Claim::Waiting
                }
            },
            staged::Entry::Queued(entry) if *entry.key() == key => Claim::Waiting,
            staged::Entry::Queued(entry) => {
                let (entry, _) = entry.remove();
                entry.insert(key, ());
                Claim::Waiting
            }
            staged::Entry::Parked(entry) => Claim::Acquired(Lease::new(entry.take().1, gate)),
        }
    }

    pub(in crate::network::listener::writer::memory) fn cancel(self, target: ready::Target<'d>) {
        let Self { gate, proof } = self;
        let key = target.key();
        // SAFETY: this synchronous cancellation consumes the exact retained slot once.
        let lease = match staged::Entry::from_source(unsafe { depot::raw::Proof::new(proof) }) {
            staged::Entry::Vacant(_) => None,
            staged::Entry::Queued(entry) if *entry.key() == key => {
                entry.clear();
                None
            }
            staged::Entry::Parked(entry) if *entry.key() == key => Some(entry.take().1),
            staged::Entry::Queued(_) | staged::Entry::Parked(_) => None,
        };
        if let Some(lease) = lease {
            gate.wake(lease);
        }
    }
}

impl<'p, 's, 'd, T, ID: Identity> Lease<'p, 's, 'd, T, ID> {
    fn new(lease: Unique<'s, T, recycle::Header<T>>, gate: &'p Gate<'s, 'd, T, ID>) -> Self {
        Self {
            lease: mem::Lease::open(Wake(gate), lease),
        }
    }

    pub(in crate::network::listener) fn get(&self) -> pin::Pin<&T> {
        self.lease.item().get()
    }

    pub(in crate::network::listener) fn get_mut(&mut self) -> pin::Pin<&mut T> {
        self.lease.item_mut().get_mut()
    }
}

impl<'s, T, ID: Identity> Return for Wake<'_, 's, '_, T, ID> {
    type Item = Unique<'s, T, recycle::Header<T>>;

    fn return_item(self, lease: Self::Item) {
        self.0.wake(lease);
    }
}

const _: () = {
    let word = size_of::<usize>();
    assert!(size_of::<Lease<'static, 'static, 'static, memory::Header, KeyTag<0>>>() == 2 * word);
    assert!(
        size_of::<Option<Lease<'static, 'static, 'static, memory::Header, KeyTag<0>>>>()
            == 2 * word
    );
    assert!(size_of::<Source<'static, 'static, 'static, memory::Header, KeyTag<0>>>() == 3 * word);
    assert!(
        size_of::<Option<Source<'static, 'static, 'static, memory::Header, KeyTag<0>>>>()
            == 3 * word
    );
};
