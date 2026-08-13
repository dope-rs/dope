use std::{cell, process};

use dope::{
    manifold::{connector::app, dispatch::typed::identity},
    net::link::egress::data,
};

use crate::{context, net::port::state};

pub(super) const CLEANUP_UNLINKED: u32 = u32::MAX;
pub(super) const CLEANUP_TAIL: u32 = u32::MAX - 1;

pub(super) struct RequestState<'d> {
    send: cell::Cell<Option<data::Buffer<'d>>>,
    staged: cell::Cell<bool>,
    pub(super) close: cell::Cell<bool>,
    pub(super) queued: cell::Cell<bool>,
    pub(super) inflight: cell::Cell<bool>,
}

pub(super) struct DrainState {
    pub(super) next: cell::Cell<u32>,
    pub(super) waiting: cell::Cell<bool>,
}

pub(super) struct Entry<'d, I: identity::Identity> {
    pub(super) token: identity::Binding<I>,
    pub(super) state: state::State<'d>,
    pub(super) root_wake: cell::Cell<Option<context::RootWaker<'d>>>,
    pub(super) requests: RequestState<'d>,
    pub(super) drain: DrainState,
}

struct SendFront<'entry, 'd>(&'entry RequestState<'d>);

impl<'d> app::RequestFront for SendFront<'_, 'd> {
    type Item = data::Buffer<'d>;

    fn take(self) -> Self::Item {
        let Some(send) = self.0.take() else {
            process::abort();
        };
        send
    }
}

impl<'d> RequestState<'d> {
    pub(super) fn pending(&self) -> bool {
        self.staged.get()
    }

    pub(super) fn take(&self) -> Option<data::Buffer<'d>> {
        let send = self.send.take();
        self.staged.set(false);
        send
    }

    pub(super) fn restore(&self, send: data::Buffer<'d>) {
        self.send.set(Some(send));
        self.staged.set(true);
    }

    pub(super) fn try_stage(&self, send: data::Buffer<'d>) -> Result<(), data::Buffer<'d>> {
        if self.pending() {
            return Err(send);
        }
        self.restore(send);
        Ok(())
    }

    pub(super) fn admit<'permit, 'queue>(
        &self,
        drain: &'permit mut app::RequestDrain<'queue, 'd, data::Buffer<'d>>,
    ) -> app::RequestAdmission<'permit, 'queue, 'd, data::Buffer<'d>, data::Buffer<'d>> {
        drain.admit(self.pending().then_some(SendFront(self)))
    }
}

impl<I: identity::Identity> Default for Entry<'_, I> {
    fn default() -> Self {
        use std::cell::Cell;

        use crate::net::port::state::State;
        Self {
            token: identity::Binding::new(),
            state: State::default(),
            root_wake: Cell::new(None),
            requests: RequestState {
                send: Cell::new(None),
                staged: Cell::new(false),
                close: Cell::new(false),
                queued: Cell::new(false),
                inflight: Cell::new(false),
            },
            drain: DrainState {
                next: Cell::new(CLEANUP_UNLINKED),
                waiting: Cell::new(false),
            },
        }
    }
}
