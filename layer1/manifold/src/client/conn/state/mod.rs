use std::{marker, mem};

use dope_core::{
    driver::{
        route::{Identity, KeyTag},
        schedule::{clock, ready},
    },
    io::socket::option,
};
use dope_net::{
    link::{
        pool::change::close::Approval,
        slot::{transmit, types},
    },
    wire::{self, terminal},
};

use crate::client::{
    app, close,
    conn::{
        self, closing, drain,
        flow::{Active, Apply, Close, Effect, Flow},
    },
};

mod work;

pub(in crate::client) use work::Taken;
pub(super) use work::Work;

struct Attached<'d, C> {
    conn: C,
    last_recv: clock::Moment<'d>,
}

pub(in crate::client) struct Enter<'d, ID: Identity> {
    brand: marker::PhantomData<fn(conn::Id<'d, ID>) -> conn::Id<'d, ID>>,
}

/// Final close decision after its I/O effect has been reconciled.
pub(in crate::client) struct Finalized<'d, ID: Identity> {
    pub(super) drain: drain::Drain<'d, ID>,
}

impl<'d, ID: Identity> Finalized<'d, ID> {
    pub(in crate::client::conn) const fn new(
        conn: conn::Id<'d, ID>,
        claim: closing::Claim,
    ) -> Self {
        Self {
            drain: drain::Drain::new(conn, claim),
        }
    }

    pub(in crate::client) fn into_drain(self) -> drain::Drain<'d, ID> {
        self.drain
    }
}

impl<'d, C, O> State<'d, C, O> {
    /// Runs an active callback and settles its close request before returning.
    pub(in crate::client) fn active<ID: Identity, W, T>(
        slot: &mut types::Connection<'_, 'd, ID, W, Self>,
        callback: impl for<'call> FnOnce(ActiveMut<'call, 'd, ID, W, C, O>) -> T,
    ) -> Active<T>
    where
        W: wire::Spec + 'd,
    {
        match closing::Callback::new(&slot.state.closing) {
            closing::Callback::Ready => {}
            closing::Callback::Sealed => return Active::Sealed,
        }
        Self::attached(slot, callback)
    }

    /// Delivers one consumed terminal without reopening finalized callbacks.
    pub(in crate::client) fn terminal<ID: Identity, W, T>(
        slot: &mut types::Connection<'_, 'd, ID, W, Self>,
        callback: impl for<'call> FnOnce(conn::Io<'call, 'd, ID, W, C, O>) -> T,
    ) -> Active<T>
    where
        W: wire::Spec + 'd,
    {
        let id = conn::Id::from_key(slot.key());
        let ready = slot.io().wake_target();
        let state = &mut slot.state;
        let Some((output, urgency)) = state.closing.terminal(ready, |target| {
            let active = state.phase.as_mut()?;
            Some(callback(conn::Io {
                id,
                conn: &mut active.conn,
                closing: conn::CloseRequest {
                    target,
                    brand: marker::PhantomData,
                },
                ready,
                spec: marker::PhantomData,
            }))
        }) else {
            return Active::Sealed;
        };
        let Some(output) = output else {
            return Active::Opening;
        };
        match urgency {
            conn::Urgency::Deferred => Active::Ready(Flow::Continue(output)),
            conn::Urgency::Immediate => {
                slot.discard();
                Active::Ready(Flow::Discarded(output))
            }
        }
    }

    fn attached<ID: Identity, W, T>(
        slot: &mut types::Connection<'_, 'd, ID, W, Self>,
        callback: impl for<'call> FnOnce(ActiveMut<'call, 'd, ID, W, C, O>) -> T,
    ) -> Active<T>
    where
        W: wire::Spec + 'd,
    {
        let id = conn::Id::from_key(slot.key());
        let ready = slot.io().wake_target();
        let active = match &mut slot.state.phase {
            Some(active) => active,
            None => return Active::Opening,
        };
        let output = callback(ActiveMut {
            id,
            active,
            closing: &mut slot.state.closing,
            ready,
            spec: marker::PhantomData,
        });
        Active::Ready(Flow::from_effect(output, Self::reconcile(slot)))
    }

    /// Runs an active close callback or settles an opening connection.
    pub(in crate::client) fn close<ID: Identity, W>(
        slot: &mut types::Connection<'_, 'd, ID, W, Self>,
        rule: close::Directive,
        callback: impl for<'call> FnOnce(ActiveMut<'call, 'd, ID, W, C, O>) -> app::CloseOutcome,
    ) -> Close
    where
        W: wire::Spec + 'd,
    {
        match closing::Callback::new(&slot.state.closing) {
            closing::Callback::Ready => {}
            closing::Callback::Sealed => return Close::Sealed,
        }
        let id = conn::Id::from_key(slot.key());
        let ready = slot.io().wake_target();
        let output = match &mut slot.state.phase {
            Some(active) => callback(ActiveMut {
                id,
                active,
                closing: &mut slot.state.closing,
                ready,
                spec: marker::PhantomData,
            }),
            None => app::CloseOutcome::Complete(rule),
        };
        let output = match output {
            app::CloseOutcome::Complete(rule) => match slot.state.closing.complete(rule) {
                conn::CloseUpdate::Applied(rule) => app::CloseOutcome::Complete(rule),
                conn::CloseUpdate::Sealed(_) => {
                    Self::reconcile(slot);
                    return Close::Sealed;
                }
            },
            app::CloseOutcome::Yield => app::CloseOutcome::Yield,
        };
        Close::Ready(Flow::from_effect(output, Self::reconcile(slot)))
    }

    /// Merges a close directive and immediately settles its I/O effect.
    pub(in crate::client) fn apply<ID: Identity, W>(
        slot: &mut types::Connection<'_, 'd, ID, W, Self>,
        rule: close::Directive,
    ) -> Apply
    where
        W: wire::Spec + 'd,
    {
        match slot.state.closing.apply(rule).outcome() {
            conn::CloseUpdate::Applied(_) => {
                Apply::Applied(Flow::from_effect((), Self::reconcile(slot)))
            }
            conn::CloseUpdate::Sealed(_) => {
                Self::reconcile(slot);
                Apply::Sealed
            }
        }
    }

    fn reconcile<ID: Identity, W>(slot: &mut types::Connection<'_, 'd, ID, W, Self>) -> Effect
    where
        W: wire::Spec + 'd,
    {
        match slot.state.closing.take_urgency() {
            conn::Urgency::Deferred => Effect::Preserved,
            conn::Urgency::Immediate => {
                slot.discard();
                Effect::Discarded
            }
        }
    }

    pub(super) fn opening(owner: O, options: option::Set) -> Self {
        Self {
            owner,
            phase: None,
            options,
            closing: conn::Closing::default(),
            work: Work::new(),
        }
    }

    pub(in crate::client) const fn options(&self) -> option::Set {
        self.options
    }

    pub(super) const fn is_attached(&self) -> bool {
        self.phase.as_ref().is_some()
    }

    pub(super) fn conn_mut(&mut self) -> Option<&mut C> {
        Some(&mut self.phase.as_mut()?.conn)
    }

    pub(super) fn record_recv(&mut self, now: clock::Moment<'d>) {
        if let Some(active) = self.phase.as_mut() {
            active.last_recv = now;
        }
    }

    pub(super) fn retire<ID: Identity>(
        mut self,
        drain: &drain::Drain<'d, ID>,
        quiesce: impl for<'a> FnOnce(app::Barrier<'a, 'd, ID>) -> app::Drain<'a, 'd, ID>,
    ) -> Approval<Self, drain::Commit<'d, ID, C>> {
        let Some(rule) = self.closing.commit(drain.rule()) else {
            return Approval::Stale(self);
        };
        let Some(active) = self.phase.take() else {
            return Approval::Ready(drain::Commit {
                app: drain::Application::Detached,
                rule,
            });
        };
        match quiesce(app::Barrier::new(drain)).resolve() {
            app::Resolution::Pending => {
                self.phase = Some(active);
                Approval::Pending(self)
            }
            app::Resolution::Ready(proof) => Approval::Ready(drain::Commit {
                app: drain::Application::Drained {
                    proof,
                    conn: active.conn,
                    transmit: self.closing.transmit,
                },
                rule,
            }),
        }
    }

    pub(super) fn fail(&mut self, failure: conn::Transmit) {
        self.closing.transmit.get_or_insert(failure);
    }

    pub(super) fn reconnect(&mut self, reason: close::Reason) -> close::Directive {
        self.closing
            .apply(close::Directive::reconnect(reason))
            .effective()
    }

    pub(super) fn observe(&mut self, kind: terminal::Kind) {
        self.closing.observe(kind)
    }
}

pub(crate) struct State<'d, C, O> {
    pub(super) owner: O,
    phase: Option<Attached<'d, C>>,
    pub(super) options: option::Set,
    pub(super) closing: conn::Closing,
    pub(super) work: Work,
}

pub(in crate::client) struct ActiveRef<'a, 'd, ID: Identity, W: wire::Spec, C, O> {
    id: conn::Id<'d, ID>,
    active: &'a Attached<'d, C>,
    spec: marker::PhantomData<fn(W, O)>,
}

impl<'a, 'd, ID: Identity, W: wire::Spec, C, O> ActiveRef<'a, 'd, ID, W, C, O> {
    pub(super) fn new(slot: &'a types::Connection<'_, 'd, ID, W, State<'d, C, O>>) -> Option<Self> {
        Some(Self {
            id: conn::Id::from_key(slot.key()),
            active: slot.state.phase.as_ref()?,
            spec: marker::PhantomData,
        })
    }

    pub(in crate::client::conn) const fn id(&self) -> conn::Id<'d, ID> {
        self.id
    }

    pub(in crate::client::conn) const fn conn(&self) -> &C {
        &self.active.conn
    }

    pub(in crate::client::conn) const fn last_recv(&self) -> clock::Moment<'d> {
        self.active.last_recv
    }
}

#[repr(transparent)]
pub(in crate::client) struct OpeningMut<'a, 'w, 'd: 'w, ID: Identity, W: wire::Spec, C, O> {
    slot: &'a mut types::Connection<'w, 'd, ID, W, State<'d, C, O>>,
}

#[must_use = "attachment outcome must be consumed"]
pub(super) enum Attachment {
    Active,
    Closing,
    Discarded,
    Sealed,
}

impl<'a, 'w, 'd: 'w, ID: Identity, W: wire::Spec, C, O> OpeningMut<'a, 'w, 'd, ID, W, C, O> {
    pub(super) fn new(
        slot: &'a mut types::Connection<'w, 'd, ID, W, State<'d, C, O>>,
    ) -> Option<Self> {
        slot.state.phase.as_ref().is_none().then_some(Self { slot })
    }

    pub(super) fn attach(
        self,
        conn: C,
        now: clock::Moment<'d>,
        callback: impl for<'call> FnOnce(ActiveMut<'call, 'd, ID, W, C, O>),
    ) -> Attachment
    where
        W: 'd,
    {
        let slot = self.slot;
        let id = conn::Id::from_key(slot.key());
        let ready = slot.io().wake_target();
        let state = &mut slot.state;
        let active = state.phase.insert(Attached {
            conn,
            last_recv: now,
        });
        match closing::Callback::new(&state.closing) {
            closing::Callback::Ready => {}
            closing::Callback::Sealed => return Attachment::Sealed,
        }
        callback(ActiveMut {
            id,
            active,
            closing: &mut state.closing,
            ready,
            spec: marker::PhantomData,
        });
        match State::reconcile(slot) {
            Effect::Preserved => {
                if slot.state.closing.is_closing() {
                    Attachment::Closing
                } else {
                    Attachment::Active
                }
            }
            Effect::Discarded => Attachment::Discarded,
        }
    }

    pub(super) const fn into_slot(
        self,
    ) -> &'a mut types::Connection<'w, 'd, ID, W, State<'d, C, O>> {
        self.slot
    }
}

pub(in crate::client::conn) enum Io<'a, 'd, ID: Identity, W: wire::Spec, C, O> {
    Attached(conn::Io<'a, 'd, ID, W, C, O>),
    Opening(&'a O),
}

impl<'a, 'd, ID: Identity, W: wire::Spec + 'd, C, O> Io<'a, 'd, ID, W, C, O> {
    pub(in crate::client::conn) fn new(
        slot: &'a mut types::Connection<'_, 'd, ID, W, State<'d, C, O>>,
    ) -> Self {
        let id = conn::Id::from_key(slot.key());
        let ready = slot.io().wake_target();
        match &mut slot.state.phase {
            Some(active) => Self::Attached(conn::Io {
                id,
                conn: &mut active.conn,
                closing: conn::CloseRequest::new(&mut slot.state.closing),
                ready,
                spec: marker::PhantomData,
            }),
            None => Self::Opening(&slot.state.owner),
        }
    }
}

pub(in crate::client) struct ActiveMut<'a, 'd, ID: Identity, W: wire::Spec, C, O> {
    id: conn::Id<'d, ID>,
    active: &'a mut Attached<'d, C>,
    closing: &'a mut conn::Closing,
    ready: ready::Target<'d>,
    spec: marker::PhantomData<fn(W, O)>,
}

impl<'a, 'd, ID: Identity, W: wire::Spec, C, O> ActiveMut<'a, 'd, ID, W, C, O> {
    pub(in crate::client::conn) const fn id(&self) -> conn::Id<'d, ID> {
        self.id
    }

    pub(in crate::client::conn) const fn conn(&self) -> &C {
        &self.active.conn
    }

    pub(in crate::client::conn) fn conn_mut(&mut self) -> &mut C {
        &mut self.active.conn
    }

    pub(in crate::client::conn) fn closing_mut(&mut self) -> &mut conn::Closing {
        self.closing
    }

    pub(in crate::client::conn) const fn transmit(&self) -> Option<&conn::Transmit> {
        self.closing.transmit.as_ref()
    }

    pub(in crate::client::conn) const fn ready(&self) -> ready::Target<'d> {
        self.ready
    }

    pub(in crate::client::conn) fn into_parts(self) -> (&'a mut C, &'a mut conn::Closing) {
        (&mut self.active.conn, self.closing)
    }

    pub(in crate::client::conn) fn into_retire(
        self,
    ) -> (&'a mut C, &'a mut conn::Closing, Enter<'d, ID>) {
        (
            &mut self.active.conn,
            self.closing,
            Enter {
                brand: marker::PhantomData,
            },
        )
    }
}

const _: () = {
    assert!(mem::size_of::<Attachment>() == 1);
    assert!(
        mem::size_of::<Option<Attached<'_, ()>>>() == mem::size_of::<Option<clock::Moment<'_>>>()
    );
    assert!(mem::size_of::<Option<conn::Transmit>>() == mem::size_of::<transmit::Invalid>());
    assert!(
        mem::size_of::<ActiveRef<'static, 'static, KeyTag<0>, wire::Identity, (), ()>>()
            == 3 * mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<ActiveMut<'static, 'static, KeyTag<0>, wire::Identity, (), ()>>()
            == 6 * mem::size_of::<usize>()
    );
};
