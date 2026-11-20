use dope_net::wire::{
    self,
    receive::{self, batch::Items},
};

use crate::app::receive::Outcome;

mod grant;

pub(crate) use grant::Grant;

pub(crate) fn drive<'input, W, S, F>(
    mut items: Items<'input, W>,
    mut state: S,
    mut recv: F,
) -> (Option<S>, Outcome)
where
    W: wire::Spec,
    F: FnMut(
        S,
        <<W::Receive as receive::Policy<W>>::Batch<'input> as receive::raw::Batch<'input>>::Item,
    ) -> (Option<S>, Outcome),
{
    for item in &mut items {
        let (next, outcome) = recv(state, item);
        let Some(next) = next else { return (None, outcome) };
        state = next;
        match outcome {
            Outcome::Continue => {}
            done @ (Outcome::CloseAfter | Outcome::Failed(_)) => {
                return (Some(state), done);
            }
        }
    }
    (Some(state), Outcome::Continue)
}
