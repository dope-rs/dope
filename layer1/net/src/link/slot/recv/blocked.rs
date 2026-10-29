use dope_core::driver::{receive::credit, route::Identity};

use crate::{
    link::pool::recv::Resident,
    wire::{self, receive::reserve},
};

pub(in crate::link) struct Blocked<
    'a,
    'w,
    'd,
    ID: Identity + 'w,
    W: wire::Spec + 'w,
    R: reserve::Try<W>,
> where
    'd: 'w,
    'w: 'a,
{
    pub(in crate::link) block: R::Block<'a, 'w, 'd, ID>,
    pub(in crate::link) credit: credit::Pending<'d, ID>,
    pub(in crate::link) resident: Resident<'a, 'd, ID>,
    pub(in crate::link) needs_cancel: bool,
}
