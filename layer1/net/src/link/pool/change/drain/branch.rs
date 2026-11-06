use dope_core::driver::route::Identity;

use crate::link::{pool::change::close::Resume, slot::transmit::outcome};

pub(in crate::link::pool::change) enum Branch<'d, ID: Identity, P, B> {
    Retired(P),
    Pending,
    Owner(outcome::Wait<'d, ID>),
    Resume(Resume<'d>),
    Released { payload: B, resume: Resume<'d> },
    Stale,
    Missing,
}
