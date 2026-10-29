use dope_core::{
    driver::{result, retained, retained::flight, route, route::kind, schedule::ready},
    io::{event::batch::Flow, fd::handles, transfer},
};

use crate::{
    link::{
        duplex::{receive, sending, state},
        event::SendSource,
    },
    wire::send::reclaim,
};

pub(in crate::link) enum Cancel<'a, 'd, C: kind::Class> {
    Idle,
    Armed(&'a mut flight::Flight<'d, C>),
}

pub(in crate::link) enum Shutdown {
    Armed,
    Busy,
    Submitted,
    Complete,
    Rejected(result::SubmitError),
}

pub(in crate::link) enum QueueError {
    Origin {
        actual: Option<SendSource>,
        expected: Option<SendSource>,
    },
    Overflow(reclaim::Overflow),
}

/// One connection's compact operation state and exact kernel authorities.
/// Flight-bearing transitions are deliberately available only through this
/// owner so phase and authority cannot be mutated through separate objects.
pub(in crate::link) struct State<'d, P: reclaim::Policy = reclaim::OnComplete> {
    pub(in crate::link) recv: receive::Receive<'d>,
    pub(in crate::link) lifecycle: state::State,
    pub(in crate::link) sending: sending::Sending<'d, P>,
    /// Reclaim acknowledgement state is owned by the same aggregate as the
    /// exact send flight, phase, origin, and lifecycle authority.
    pub(in crate::link) acknowledgements: P::AckState,
}

impl<'d, P: reclaim::Policy> State<'d, P> {
    pub(super) fn new() -> Self {
        Self {
            recv: receive::Receive::new(),
            lifecycle: state::State::new(),
            sending: sending::Sending::new(),
            acknowledgements: P::AckState::default(),
        }
    }

    pub(in crate::link) fn arm_recv(&mut self, flight: flight::Flight<'d, kind::Recv>) {
        self.recv.armed(flight);
    }

    /// Observes one fresh backend completion exactly once. Terminal authority
    /// is consumed before delivery can defer or fail.
    pub(in crate::link) fn observe_recv(
        &mut self,
        flow: Flow,
        close: bool,
    ) -> Option<receive::Observation> {
        self.recv.observe(flow, close)
    }

    pub(in crate::link) fn cancel_recv(&mut self) -> Cancel<'_, 'd, kind::Recv> {
        self.recv.cancel()
    }

    pub(in crate::link) fn capture(&mut self, handle: ready::Handle<'d>) {
        self.recv.pause();
        handle.activate();
    }

    pub(in crate::link) fn submission(
        &mut self,
        source: sending::Source,
    ) -> sending::Submission<'_, 'd, P> {
        sending::Submission::new(&mut self.sending, &mut self.lifecycle, source)
    }

    pub(in crate::link) fn shutdown<Tag: route::Tag + route::Select<kind::Send>>(
        &mut self,
        driver: &mut retained::Context<'_, '_, 'd>,
        slots: &flight::Slots<'d, Tag>,
        fd: &handles::Descriptor<'d>,
        target: route::Target<'d, Tag>,
    ) -> Shutdown {
        match self.lifecycle.close.take_write() {
            state::Shutdown::Pending => return Shutdown::Busy,
            state::Shutdown::Ready => {}
            state::Shutdown::Submitted => return Shutdown::Submitted,
            state::Shutdown::Complete => return Shutdown::Complete,
        }
        match self.sending.shutdown(driver, slots, fd, target) {
            sending::Shutdown::Armed => {
                self.lifecycle.close.arm_write();
                Shutdown::Armed
            }
            sending::Shutdown::Busy => Shutdown::Busy,
            sending::Shutdown::Rejected(error) => Shutdown::Rejected(error),
        }
    }

    pub(in crate::link) fn finish_send(&mut self) -> sending::Done {
        self.sending.done()
    }

    pub(in crate::link) fn cancel_send(&mut self) -> Cancel<'_, 'd, kind::Send> {
        self.sending.cancel()
    }

    pub(in crate::link) fn queue_submit(
        &mut self,
        credit: P::Credit,
    ) -> Result<reclaim::Submission, QueueError> {
        let state = self.queue_state(Some(SendSource::Queued))?;
        let (next, action) = P::submitted(state, credit).map_err(QueueError::Overflow)?;
        *state = next;
        Ok(action)
    }

    pub(in crate::link) fn queue_unsent(
        &mut self,
        credit: P::Credit,
    ) -> Result<transfer::Len, QueueError> {
        let state = self.queue_state(None)?;
        let (next, free) = P::unsubmitted(state, credit).map_err(QueueError::Overflow)?;
        *state = next;
        Ok(free)
    }

    pub(in crate::link) fn queue_done(&mut self) -> Result<reclaim::Completion, QueueError> {
        Ok(P::complete(self.queue_state(None)?))
    }

    fn queue_state(
        &mut self,
        expected: Option<SendSource>,
    ) -> Result<&mut P::AckState, QueueError> {
        let actual = self.sending.origin();
        if actual != expected {
            return Err(QueueError::Origin { actual, expected });
        }
        Ok(&mut self.acknowledgements)
    }
}

const _: () = {
    use std::mem;

    let fields = mem::size_of::<sending::Sending<'_>>()
        + mem::size_of::<receive::Receive<'_>>()
        + mem::size_of::<state::State>();
    let align = mem::align_of::<State<'_>>();
    assert!(mem::size_of::<State<'_>>() == fields.div_ceil(align) * align);
    let submit = mem::size_of::<sending::Sending<'_, reclaim::OnSubmit>>()
        + mem::size_of::<receive::Receive<'_>>()
        + mem::size_of::<state::State>()
        + mem::size_of::<<reclaim::OnSubmit as reclaim::Policy>::AckState>();
    let align = mem::align_of::<State<'_, reclaim::OnSubmit>>();
    assert!(mem::size_of::<State<'_, reclaim::OnSubmit>>() == submit.div_ceil(align) * align);
};
