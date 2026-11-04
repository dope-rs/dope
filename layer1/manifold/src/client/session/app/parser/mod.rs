//! Framed and duplex ingress over the exact retained receive owner.

use std::ops::ControlFlow;

use dope_core::driver::{route::Identity, schedule::app};
use dope_net::wire;

use crate::{
    app::receive::Failure,
    client::session::{
        self,
        response::{self, flow, work},
    },
};

mod core;
pub(super) mod outcomes;
pub(super) use core::Parser;

use outcomes::Run;

impl<'a, 'connection, 'e, 'w, 'd: 'w, ID: Identity, N: session::Parser<'d, ID>, W: wire::Spec + 'd>
    Parser<'a, 'connection, 'e, 'w, 'd, ID, N, W>
{
    pub(super) fn run_admitted<'work>(
        mut self,
        state: &mut <N::Response as response::Plan>::Flow,
        work: app::Work<'work, 'd>,
        permit: app::Permit<'work, 'd>,
    ) -> Run {
        let mut work = work::Reserve::new(work, permit);
        self.run(state, flow::Entry::Resume, &mut work)
    }

    pub(super) fn run_retained(
        mut self,
        state: &mut <N::Response as response::Plan>::Flow,
        mut work: app::Work<'_, 'd>,
    ) -> Run {
        self.run(state, flow::Entry::Chunk, &mut work)
    }

    fn run<A: work::Admission>(
        &mut self,
        state: &mut <N::Response as response::Plan>::Flow,
        entry: flow::Entry,
        work: &mut A,
    ) -> Run {
        let next = match flow::Begin::begin(state, self.owner, work, self.context, entry) {
            flow::Start::Ready => self.run_ready(state, false, work),
            flow::Start::Eof => self.run_ready(state, true, work),
            flow::Start::Idle | flow::Start::Finishing | flow::Start::Finished => Run::Complete,
            flow::Start::Blocked => Run::Blocked,
            flow::Start::Yield => Run::Yield,
            flow::Start::Close => Run::Close,
            flow::Start::Contract => Run::Failed(Failure::Contract),
        };
        if !matches!(next, Run::Yield | Run::Blocked) {
            self.discard();
        }
        next
    }

    fn run_ready<A: work::Admission>(
        &mut self,
        state: &mut <N::Response as response::Plan>::Flow,
        eof: bool,
        work: &mut A,
    ) -> Run {
        loop {
            match self.step(state, eof, work) {
                ControlFlow::Continue(()) => {}
                ControlFlow::Break(next) => return next,
            }
        }
    }
}
