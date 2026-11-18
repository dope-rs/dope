use std::mem;

use dope_core::driver::route::{Identity, KeyTag};
use dope_net::link::egress::data;
use memory::access;
use o3::buffer::resident;

use crate::network::listener::{conn, writer::memory};

pub(in crate::network::listener) struct Send<
    'p,
    's,
    'd,
    ID: Identity,
    B: data::raw::Payload<'d> = resident::Shared<'d>,
> {
    state: State<'p, 's, 'd, ID, B>,
}

enum State<'p, 's, 'd, ID: Identity, B: data::raw::Payload<'d>> {
    Queued,
    Direct(access::Direct<'p, 's, 'd, ID, B>),
    Failed(conn::Transmit),
}

#[repr(u8)]
pub(in crate::network::listener::writer) enum Mode {
    Queued,
    Direct,
}

impl<'d, ID: Identity, B: data::raw::Payload<'d>> Default for Send<'_, '_, 'd, ID, B> {
    fn default() -> Self {
        Self {
            state: State::Queued,
        }
    }
}

impl<'p, 's, 'd, ID: Identity, B: data::raw::Payload<'d>> Send<'p, 's, 'd, ID, B> {
    pub(in crate::network::listener) fn begin(
        &mut self,
        direct: access::Direct<'p, 's, 'd, ID, B>,
    ) {
        self.state = State::Direct(direct);
    }

    pub(in crate::network::listener::writer) fn prepare(
        &mut self,
    ) -> Result<access::Prepared<'_>, access::Fault> {
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
            State::Direct(state) => {
                let error = state.progress.error();
                (state.retire(), error)
            }
            State::Queued => (None, None),
            State::Failed(error) => (None, Some(error)),
        }
    }

    pub(in crate::network::listener) fn fail(&mut self, failure: conn::Transmit) -> Option<B> {
        self.take(Some(failure))
    }

    pub(in crate::network::listener::writer) fn invalidate(&mut self, fault: access::Fault) {
        if let Some(progress) = self.progress() {
            progress.invalidate(fault);
        }
    }

    pub(in crate::network::listener::writer) fn progress(
        &mut self,
    ) -> Option<&mut access::Progress> {
        match &mut self.state {
            State::Direct(state) => Some(&mut state.progress),
            State::Queued | State::Failed(_) => None,
        }
    }

    pub(in crate::network::listener::writer) fn take(
        &mut self,
        failure: Option<conn::Transmit>,
    ) -> Option<B> {
        let (body, error) = self.retire();
        if let Some(error) = error.or(failure) {
            self.state = State::Failed(error);
        }
        body
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
