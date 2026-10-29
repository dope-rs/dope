use dope_core::driver::route::Identity;

use crate::link::pool::change::close::{Deferred, Resume};

pub(in crate::link::pool::change) enum Branch<'d, ID: Identity, P, B> {
    Retired(P),
    Pending(Deferred<'d, ID>),
    Resume(Resume<'d>),
    Released { payload: B, resume: Resume<'d> },
    Stale,
    Missing,
}
