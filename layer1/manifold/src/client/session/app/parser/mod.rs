//! Framed and duplex ingress over the exact retained receive owner.

use std::ops::ControlFlow;

use dope_core::driver::{route::Identity, schedule::app};
use dope_net::wire;
use o3::buffer::resident;

use crate::{
    app::receive::Failure,
    client::session::{
        self,
        response::{self, flow, work},
        turns,
    },
};

mod core;
pub(super) mod outcomes;
use core::Core;

use outcomes::Run;

pub(super) enum Retire {
    Run { next: Run, ended: bool },
    Finishing,
    Finished,
}

pub(super) struct Parser<
    'a,
    'work,
    'connection,
    'e,
    'w,
    'd: 'w,
    ID: Identity,
    N: session::Parser<'d, ID>,
    W: wire::Spec + 'd,
> {
    core: Core<'a, 'connection, 'e, 'w, 'd, ID, N, W>,
    work: app::Work<'work, 'd>,
}

impl<
    'a,
    'work,
    'connection,
    'e,
    'w,
    'd: 'w,
    ID: Identity,
    N: session::Parser<'d, ID>,
    W: wire::Spec + 'd,
> Parser<'a, 'work, 'connection, 'e, 'w, 'd, ID, N, W>
{
    pub(super) fn new(
        owner: &'a mut N,
        ingress: &'a mut session::Ingress<'w, 'd, ID, N, W>,
        budget: &'a resident::Budget<'d>,
        context: &'a mut turns::Context<'connection, 'e, 'd, N::ConnState, N::Send, ID>,
        work: app::Work<'work, 'd>,
    ) -> Self {
        Self {
            core: Core::new(owner, budget, ingress, context),
            work,
        }
    }

    pub(super) fn run_admitted(
        self,
        state: &mut <N::Response as response::Plan>::Flow,
        permit: app::Permit<'work, 'd>,
    ) -> Run {
        let Self { mut core, work } = self;
        let mut work = work::Reserve::new(work, permit);
        Self::run(&mut core, state, flow::Entry::Resume, &mut work)
    }

    pub(super) fn run_retained(self, state: &mut <N::Response as response::Plan>::Flow) -> Run {
        let Self { mut core, mut work } = self;
        Self::run(&mut core, state, flow::Entry::Chunk, &mut work)
    }

    pub(super) fn retire(
        self,
        state: &mut <N::Response as response::Plan>::Flow,
        entry: flow::Entry,
    ) -> Retire {
        let Self { mut core, mut work } = self;
        match flow::Begin::begin(state, core.owner, &mut work, core.context, entry) {
            flow::Start::Ready => Retire::Run {
                next: Self::run_ready(&mut core, state, false, &mut work),
                ended: false,
            },
            flow::Start::Eof => Retire::Run {
                next: Self::run_ready(&mut core, state, true, &mut work),
                ended: true,
            },
            flow::Start::Idle => Retire::Run {
                next: Run::Complete,
                ended: false,
            },
            flow::Start::Blocked => Retire::Run {
                next: Run::Blocked,
                ended: false,
            },
            flow::Start::Yield => Retire::Run {
                next: Run::Yield,
                ended: false,
            },
            flow::Start::Close => Retire::Run {
                next: Run::Close,
                ended: false,
            },
            flow::Start::Contract => Retire::Run {
                next: Run::Failed(Failure::Contract),
                ended: false,
            },
            flow::Start::Finishing => Retire::Finishing,
            flow::Start::Finished => Retire::Finished,
        }
    }

    pub(super) fn finish(self, state: &mut <N::Response as response::Plan>::Flow) -> Run {
        let Self { mut core, work } = self;
        core.finish(state, work)
    }

    fn run<A: work::Admission>(
        core: &mut Core<'_, 'connection, 'e, 'w, 'd, ID, N, W>,
        state: &mut <N::Response as response::Plan>::Flow,
        entry: flow::Entry,
        work: &mut A,
    ) -> Run {
        let next = match flow::Begin::begin(state, core.owner, work, core.context, entry) {
            flow::Start::Ready => Self::run_ready(core, state, false, work),
            flow::Start::Eof => Self::run_ready(core, state, true, work),
            flow::Start::Idle | flow::Start::Finishing | flow::Start::Finished => Run::Complete,
            flow::Start::Blocked => Run::Blocked,
            flow::Start::Yield => Run::Yield,
            flow::Start::Close => Run::Close,
            flow::Start::Contract => Run::Failed(Failure::Contract),
        };
        if !matches!(next, Run::Yield | Run::Blocked) {
            core.discard();
        }
        next
    }

    fn run_ready<A: work::Admission>(
        core: &mut Core<'_, 'connection, 'e, 'w, 'd, ID, N, W>,
        state: &mut <N::Response as response::Plan>::Flow,
        eof: bool,
        work: &mut A,
    ) -> Run {
        loop {
            match core.run_ready(state, eof, work) {
                ControlFlow::Continue(()) => {}
                ControlFlow::Break(next) => return next,
            }
        }
    }
}
