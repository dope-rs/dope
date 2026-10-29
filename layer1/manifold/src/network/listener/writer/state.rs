use std::mem;

use dope_core::driver::route::{Identity, KeyTag};
use dope_net::link::{self, egress::data};
use memory::access;
use o3::buffer::resident;

use crate::network::listener::{conn, writer::memory};

pub(in crate::network::listener) struct Send<'p, 's, 'd, ID: Identity, B = resident::Shared<'d>> {
    state: State<'p, 's, 'd, ID, B>,
}

enum State<'p, 's, 'd, ID: Identity, B> {
    Queued,
    Direct(access::Direct<'p, 's, 'd, ID, B>),
    Failed(conn::Transmit),
}

#[repr(u8)]
pub(in crate::network::listener::writer) enum Mode {
    Queued,
    Direct,
}

pub(in crate::network::listener) enum Settled<B> {
    More,
    Done(Option<B>),
    Invalid,
}

impl<ID: Identity, B> Default for Send<'_, '_, '_, ID, B> {
    fn default() -> Self {
        Self {
            state: State::Queued,
        }
    }
}

impl<'p, 's, 'd, ID: Identity, B> Send<'p, 's, 'd, ID, B> {
    pub(in crate::network::listener) fn begin(
        &mut self,
        direct: access::Direct<'p, 's, 'd, ID, B>,
    ) {
        self.state = State::Direct(direct);
    }

    pub(in crate::network::listener::writer) fn prepare(
        &mut self,
    ) -> Result<access::Prepared<'_>, access::Fault>
    where
        B: data::raw::Payload<'d>,
    {
        match &mut self.state {
            State::Direct(state) => state.prepare(),
            State::Queued | State::Failed(_) => Ok(access::Prepared::Idle),
        }
    }

    pub(in crate::network::listener) fn error(&self) -> Option<conn::Transmit> {
        match &self.state {
            State::Queued => None,
            State::Direct(state) => state.progress.error(),
            State::Failed(error) => Some(*error),
        }
    }

    /// Retires direct-send ownership after the net layer has quiesced its SQE.
    pub(in crate::network::listener) fn retire(&mut self) -> (Option<B>, Option<conn::Transmit>) {
        match mem::replace(&mut self.state, State::Queued) {
            State::Queued => (None, None),
            State::Direct(mut state) => (state.take_source(), state.progress.error()),
            State::Failed(error) => (None, Some(error)),
        }
    }

    pub(in crate::network::listener) fn release(&mut self) -> Option<B> {
        match mem::replace(&mut self.state, State::Queued) {
            State::Queued => None,
            State::Direct(mut state) => {
                let error = state.progress.error();
                let source = state.take_source();
                if let Some(error) = error {
                    self.state = State::Failed(error);
                }
                source
            }
            State::Failed(error) => {
                self.state = State::Failed(error);
                None
            }
        }
    }

    pub(in crate::network::listener) fn fail(&mut self, failure: conn::Transmit) -> Option<B> {
        let (source, error) = self.retire();
        self.state = State::Failed(error.unwrap_or(failure));
        source
    }

    pub(in crate::network::listener::writer) fn invalidate(&mut self, fault: access::Fault) {
        if let State::Direct(state) = &mut self.state {
            state.progress.invalidate(fault);
        }
    }

    pub(in crate::network::listener) fn settle(
        &mut self,
        done: Option<link::Consumed>,
    ) -> Settled<B> {
        let State::Direct(state) = &mut self.state else {
            return Settled::Invalid;
        };
        state.progress.settle(done);
        if state.progress.error().is_some() {
            return Settled::Invalid;
        }
        if state.progress.has_more() {
            Settled::More
        } else {
            Settled::Done(self.retire().0)
        }
    }

    pub(in crate::network::listener::writer) fn motion(&self) -> access::Motion {
        match &self.state {
            State::Queued | State::Failed(_) => access::Motion::Empty,
            State::Direct(state) => state.progress.motion(),
        }
    }

    pub(in crate::network::listener::writer) const fn mode(&self) -> Mode {
        match self.state {
            State::Queued => Mode::Queued,
            State::Direct(_) | State::Failed(_) => Mode::Direct,
        }
    }
}

const _: () = assert!(
    mem::size_of::<Send<'static, 'static, 'static, KeyTag<0>>>()
        == mem::size_of::<
            access::Direct<'static, 'static, 'static, KeyTag<0>, resident::Shared<'static>>,
        >()
);
const _: () = {
    assert!(mem::size_of::<Mode>() == mem::size_of::<bool>());
    assert!(mem::align_of::<Mode>() == mem::align_of::<bool>());
};
