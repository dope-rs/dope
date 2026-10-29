use std::{hash, mem};

use dope::runtime::random::{self, Domain, Namespace};

use crate::wire::tx;

const STREAM_DOMAIN: Domain = Namespace::new(*b"dns\0str\0").fixed();

const CONNECTION_LANE: u64 = u64::from_be_bytes(*b"conn IDs");

pub(crate) struct Tag;

impl random::Purpose for Tag {
    const DOMAIN: Domain = STREAM_DOMAIN;
}

/// Driver-bound key for DNS stream transaction identities.
pub(crate) struct Stream<'d> {
    root: random::Root<'d, Tag>,
}

impl<'d> Stream<'d> {
    pub(crate) const fn new(root: random::Root<'d, Tag>) -> Self {
        Self { root }
    }

    pub(crate) fn start<T: hash::Hash + ?Sized>(&self, conn: &T) -> tx::Id {
        let [_, _, _, _, _, _, a, b] = self.root.hash::<CONNECTION_LANE, _>(conn).into_bytes();
        tx::Id::from_bytes([a, b])
    }
}

const _: () = assert!(mem::size_of::<Stream<'static>>() == 2 * mem::size_of::<u64>());
