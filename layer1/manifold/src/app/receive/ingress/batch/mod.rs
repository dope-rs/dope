use std::convert;

use dope_net::wire::{
    self,
    receive::{self, batch::Items},
};

use crate::app::receive::{Outcome, ingress::Progress};

mod grant;

pub(crate) use grant::Grant;

pub(crate) fn drive<'input, W, F>(
    mut items: Items<'input, W>,
    mut recv: F,
) -> Progress<convert::Infallible>
where
    W: wire::Spec,
    F: FnMut(
        <<W::Receive as receive::Policy<W>>::Batch<'input> as receive::raw::Batch<'input>>::Item,
    ) -> Outcome,
{
    for item in &mut items {
        match recv(item) {
            Outcome::Continue => {}
            done @ (Outcome::CloseAfter | Outcome::Failed(_)) => return Progress::Done(done),
        }
    }
    Progress::Done(Outcome::Continue)
}
