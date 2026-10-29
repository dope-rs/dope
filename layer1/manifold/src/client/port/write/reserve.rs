use std::mem;

use dope_core::driver::route::{Identity, KeyTag};
use dope_net::link::egress::{self, load, reserve::error};

use crate::client::port::write;

/// Result of owning one exact source-lane payload.
pub enum Reserve<'a, 'token, 'e, 'd, B, ID: Identity = KeyTag<0>> {
    Item(write::Hold<'a, 'token, 'e, 'd, B, ID>),
    Unknown(B),
    Inactive(B),
    Rejected(error::Capacity<B>),
    Invalid(egress::Rejected<egress::Load<B>, load::Error>),
    Empty(egress::Empty<B>),
}

const _: () = {
    const HOLD: usize = mem::size_of::<write::Hold<'_, '_, '_, '_, (), KeyTag<0>>>();
    const REJECTED: usize = mem::size_of::<egress::Rejected<egress::Load<()>>>();
    const PAYLOAD: usize = if HOLD > REJECTED { HOLD } else { REJECTED };
    assert!(
        mem::size_of::<Reserve<'_, '_, '_, '_, (), KeyTag<0>>>()
            <= PAYLOAD + mem::size_of::<usize>()
    );
};
