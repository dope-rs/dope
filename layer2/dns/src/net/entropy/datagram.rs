use std::mem;

use dope::runtime::random::{self, Domain, Namespace};
use random::Entropy;

use crate::wire::{cookies, tx};

const DATAGRAM_DOMAIN: Domain = Namespace::new(*b"dns\0dgr\0").fixed();

const TRANSACTION_LANE: u64 = u64::from_be_bytes(*b"transact");
const COOKIE_LANE: u64 = u64::from_be_bytes(*b"cookies\0");

pub(crate) struct Tag;

impl random::Purpose for Tag {
    const DOMAIN: Domain = DATAGRAM_DOMAIN;
}

/// Driver-bound keys for DNS datagram identities and cookies.
pub(crate) struct Datagram<'d> {
    root: random::Root<'d, Tag>,
}

impl<'d> Datagram<'d> {
    pub(crate) const fn new(root: random::Root<'d, Tag>) -> Self {
        Self { root }
    }

    pub(crate) fn start(&self, cursor: u64) -> tx::Id {
        let [_, _, _, _, _, _, a, b] = self.root.word::<TRANSACTION_LANE>(cursor).into_bytes();
        tx::Id::from_bytes([a, b])
    }

    pub(crate) fn cookie(&self, input: u64) -> cookies::Client {
        cookies::Client::from_bytes(self.root.permute::<COOKIE_LANE>(input).into_bytes())
    }
}

const _: () = assert!(mem::size_of::<Datagram<'static>>() == 2 * mem::size_of::<u64>());
