use dope_core::driver::result;

use crate::wire::send::reclaim;

pub(crate) struct Attempt<P: reclaim::Policy> {
    credit: P::Credit,
    state: State,
}

#[derive(Clone, Copy)]
pub(crate) enum State {
    Submitted,
    Rejected(result::SubmitError),
    Idle,
}

impl<P: reclaim::Policy> Attempt<P> {
    pub(crate) fn submitted(credit: P::Credit) -> Self {
        Self {
            credit,
            state: State::Submitted,
        }
    }

    pub(crate) fn rejected(credit: P::Credit, error: result::SubmitError) -> Self {
        Self {
            credit,
            state: State::Rejected(error),
        }
    }

    pub(crate) fn idle(credit: P::Credit) -> Self {
        Self {
            credit,
            state: State::Idle,
        }
    }

    pub(crate) fn into_parts(self) -> (P::Credit, State) {
        (self.credit, self.state)
    }
}

const _: () =
    assert!(std::mem::size_of::<Attempt<reclaim::OnSubmit>>() <= 2 * std::mem::size_of::<usize>());
const _: () =
    assert!(std::mem::size_of::<Attempt<reclaim::OnComplete>>() == std::mem::size_of::<State>());
