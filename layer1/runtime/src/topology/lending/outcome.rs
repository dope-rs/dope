use std::marker;

use dope_core::Errno;

use crate::{
    executor::{self, drive, drive::clock, session, session::run},
    shutdown,
    topology::lending,
};

type Executed<R> = run::Outcome<R, clock::Wall>;

pub(super) enum State<'issued, 'd, O: executor::raw::Layout<'d>, E, R> {
    Rejected(E),
    Executed(
        executor::raw::Parent<'issued, 'd, O>,
        Executed<R>,
        Option<Errno>,
    ),
}

/// Retains the exact provider until all callback-local client state has ended.
#[must_use]
pub struct Outcome<
    'issued,
    'life: 'issued,
    'd: 'life,
    O,
    Q,
    E,
    R = Result<shutdown::Requested<Q>, drive::Error>,
> where
    O: executor::raw::Layout<'d>,
{
    pub(super) core: &'issued mut session::Core<'life, 'd, Q>,
    pub(super) state: State<'issued, 'd, O, E, R>,
    pub(super) source: marker::PhantomData<fn(&'issued ()) -> &'issued ()>,
}

impl<'issued, 'life: 'issued, 'd: 'life, O, Q, E, R> Outcome<'issued, 'life, 'd, O, Q, E, R>
where
    O: lending::raw::Resume<'d>,
{
    pub(crate) fn rejected(
        core: &'issued mut session::Core<'life, 'd, Q>,
        error: E,
        source: marker::PhantomData<fn(&'issued ()) -> &'issued ()>,
    ) -> Self {
        Self {
            core,
            state: State::Rejected(error),
            source,
        }
    }

    pub(crate) fn executed(
        core: &'issued mut session::Core<'life, 'd, Q>,
        parent: executor::raw::Parent<'issued, 'd, O>,
        outcome: Executed<R>,
        disarm: Option<Errno>,
    ) -> Self {
        Self {
            core,
            state: State::Executed(parent, outcome, disarm),
            source: marker::PhantomData,
        }
    }

    pub(crate) fn finish<'report>(
        self,
        report: &'report mut session::Completion,
    ) -> lending::Result<'report, E, R> {
        match self.state {
            State::Rejected(error) => Err(lending::Error::Build(error)),
            State::Executed(parent, outcome, disarm) => {
                run::finish_parent(self.core, parent, report, outcome, disarm, drive::tick)
                    .map_err(lending::Error::Run)
            }
        }
    }
}
