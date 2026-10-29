use dope_core::driver::route::Identity;

use crate::{
    client::session::{
        protocol,
        response::{Committed, Item, Settlement, work},
        turns,
    },
    service::bindings::endpoint::State,
};

/// Consumes post-commit work with the original endpoint and driver proofs.
pub trait Action<'life, 'd: 'life, P: ?Sized, L, ID: Identity, const MAX: usize>: Sized
where
    L: protocol::Spec<'life, 'd, P, ID, MAX>,
{
    fn commit<'e, A: work::Admission>(
        item: Committed<Self>,
        owner: &mut State<'life, 'd, P, L, ID, MAX>,
        work: &mut A,
        ctx: &mut turns::Context<'_, 'e, 'd, L::ConnState, L::Send, ID>,
    ) -> Settlement<Self> {
        Self::settle(item.into_item(), owner, work, ctx)
    }

    fn settle<'e, A: work::Admission>(
        item: Item<Self>,
        owner: &mut State<'life, 'd, P, L, ID, MAX>,
        work: &mut A,
        ctx: &mut turns::Context<'_, 'e, 'd, L::ConnState, L::Send, ID>,
    ) -> Settlement<Self>;
}
