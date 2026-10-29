use std::ops::ControlFlow;

use dope_core::{
    driver::{
        retained::{
            self,
            flight::{self, cancel::Outcome},
        },
        route::{self, Identity, kind},
        schedule::maint,
    },
    io::event::net::recv::ControlKind,
};
use o3::base::Call as _;
use wire::{receive::Policy, send::Store as _};

use crate::{
    link::{
        duplex::{
            Cancel,
            state::{self, Ownership},
        },
        egress::{self, reserve::demand},
        event,
        pool::{
            change::{
                close::{
                    Approval, Commit, Decision, Deferred, Failed, Failure, Io, Owner, Phase, Resume,
                },
                drain::Branch,
            },
            recv::{
                self, input,
                storage::{queue::Ops, tail},
            },
            send::Finish,
        },
        setup,
        slot::{
            recv::{Capture, Retirement},
            types,
        },
    },
    wire,
};

pub(super) enum Step<'d, ID: Identity, R, B> {
    Continue,
    Done(Branch<'d, ID, R, B>),
}

pub(in crate::link::pool::change) struct Storage<
    'a,
    'p,
    'w,
    'd,
    ID: Identity + 'w,
    W: wire::Spec + 'w + 'd,
    S,
    M: input::Mode<W>,
    B,
    const IOV: usize,
    D: demand::Mode,
> where
    'd: 'w,
{
    pub(in crate::link::pool::change) entry:
        tail::Cleared<'a, types::Connection<'w, 'd, ID, W, S>, ID>,
    pub(in crate::link::pool::change) egress: &'a mut egress::Storage<'p, 'd, ID, B, IOV, D>,
    pub(in crate::link::pool::change) runtime: &'a mut W::Runtime<'w, 'd, ID>,
    pub(in crate::link::pool::change) recv: recv::Borrow<'a, 'd, ID, M::Ingress>,
}

pub(in crate::link::pool::change) struct Turn<'a, 'm, 'r, 'o, 'd, ID: Identity>
where
    'd: 'o,
{
    pub(in crate::link::pool::change) target: route::Target<'d, ID>,
    pub(in crate::link::pool::change) work: maint::Work<'m, 'd>,
    pub(in crate::link::pool::change) driver: &'a mut retained::Context<'r, 'o, 'd>,
}

pub(in crate::link::pool::change) struct Call<'a, 'd, ID: Identity, C, X, O, K, F> {
    pub(in crate::link::pool::change) flights: &'a flight::Slots<'d, ID>,
    pub(in crate::link::pool::change) claim: &'a C,
    pub(in crate::link::pool::change) ctx: &'a mut X,
    pub(in crate::link::pool::change) owner: &'a mut O,
    pub(in crate::link::pool::change) commit: &'a mut K,
    pub(in crate::link::pool::change) failed: &'a mut F,
}

pub(in crate::link::pool::change) struct Plan<
    'a,
    'm,
    'r,
    'o,
    'p,
    'w,
    'd,
    ID: Identity + 'w,
    W: wire::Spec + 'w + 'd,
    S,
    M: input::Mode<W>,
    B,
    const IOV: usize,
    D: demand::Mode,
    C,
    X,
    O,
    K,
    F,
> where
    'd: 'w + 'o,
{
    pub(in crate::link::pool::change) storage: Storage<'a, 'p, 'w, 'd, ID, W, S, M, B, IOV, D>,
    pub(in crate::link::pool::change) turn: Turn<'a, 'm, 'r, 'o, 'd, ID>,
    pub(in crate::link::pool::change) call: Call<'a, 'd, ID, C, X, O, K, F>,
}

impl<
    'a,
    'm,
    'r,
    'o,
    'p,
    'w,
    'd,
    ID: Identity + 'w,
    W,
    S,
    M,
    B,
    const IOV: usize,
    D,
    C,
    X,
    O,
    K,
    F,
    R,
> Plan<'a, 'm, 'r, 'o, 'p, 'w, 'd, ID, W, S, M, B, IOV, D, C, X, O, K, F>
where
    'd: 'w + 'o,
    W: wire::Spec + 'w + 'd,
    M: input::Mode<W>,
    D: demand::Mode,
    F: for<'x, 'c, 'i, 'v> FnMut(
        Failed<'x, 'c, 'i, 'v, 'r, 'o, 'p, 'w, 'd, ID, W, S, B, IOV, D, C, X>,
    ),
    O: for<'x, 'c, 'f, 's, 'v> FnMut(
        Owner<'x, 'c, 'f, 's, 'v, 'r, 'o, 'w, 'd, ID, W, S, C, X>,
    ) -> Decision<'d, ID>,
    K: for<'x, 'c, 'v> FnMut(Commit<'x, 'c, 'v, 'r, 'o, 'd, C, X, S>) -> Approval<S, R>,
{
    pub(super) fn pop(&mut self) -> Step<'d, ID, R, B>
    where
        <W::Receive as Policy<W>>::Eof: Capture<W>,
    {
        match self.storage.recv.storage.prepare_pop() {
            Some(front) => {
                let done = front.pop();
                match done.classify() {
                    event::Kind::Data(..) => {}
                    event::Kind::Control(ctrl) => {
                        if self
                            .turn
                            .target
                            .operation::<kind::Recv>()
                            .matches(ctrl.token())
                        {
                            let retired = ctrl.dispatch(|kind| {
                                match self.storage.entry.get_mut().retire_recv(kind) {
                                    Retirement::Ready => ControlFlow::Continue(Ok(())),
                                    Retirement::Failed(error) => ControlFlow::Continue(Err(error)),
                                    Retirement::Exhausted(proof) => {
                                        ControlFlow::Break((ControlKind::Exhausted(proof), ()))
                                    }
                                    Retirement::Starved(error) => {
                                        ControlFlow::Break((ControlKind::Starved(error), ()))
                                    }
                                }
                            });
                            match retired {
                                ControlFlow::Continue(Ok(())) => {}
                                ControlFlow::Continue(Err(error)) => {
                                    return Step::Done(self.fail(Failure::Receive(error)));
                                }
                                ControlFlow::Break((state, ())) => {
                                    self.turn.driver.driver().reclaim(state.retire())
                                }
                            }
                        } else {
                            self.turn.driver.driver().reclaim(ctrl.retire());
                        }
                    }
                }
                Step::Done(Branch::Resume(Resume::new(self.storage.entry.get())))
            }
            None => Step::Continue,
        }
    }

    pub(super) fn fail(&mut self, cause: Failure) -> Branch<'d, ID, R, B> {
        (self.call.failed)((
            self.call.ctx,
            self.call.claim,
            Io {
                cause,
                connection: self.storage.entry.get_mut(),
                egress: self.storage.egress,
            },
            self.turn.driver,
        ));
        Branch::Resume(Resume::new(self.storage.entry.get()))
    }

    pub(super) fn advance(&mut self) -> Step<'d, ID, R, B> {
        let next = Resume::new(self.storage.entry.get());
        let slot = self.storage.entry.get_mut();
        if slot.engine.operations.sending.is_inflight() {
            if slot.engine.operations.lifecycle.close.can_cancel() {
                let engine = &mut slot.engine;
                let flight = match engine.operations.cancel_send() {
                    Cancel::Armed(flight) => flight,
                    Cancel::Idle => return Step::Done(Branch::Resume(next)),
                };
                match flight.try_stop(self.turn.driver.driver()) {
                    Ok(Outcome::Submitted | Outcome::Queued | Outcome::Completed) => {}
                    Ok(Outcome::Deferred) => {
                        return Step::Done(Branch::Resume(next));
                    }
                    Err(cause) => {
                        engine.operations.lifecycle.close.deny_cancel();
                        self.turn.driver.rejected(cause);
                        return Step::Done(Branch::Pending(Deferred::Send));
                    }
                }
                engine.operations.lifecycle.close.mark_cancel();
            }
            return Step::Done(Branch::Pending(Deferred::Send));
        }
        slot.engine.operations.recv.pause();
        if slot.engine.operations.recv.has_cancel() {
            let next = Resume::new(self.storage.entry.get());
            let engine = &mut self.storage.entry.get_mut().engine;
            let flight = match engine.operations.cancel_recv() {
                Cancel::Armed(flight) => flight,
                Cancel::Idle => return Step::Done(Branch::Resume(next)),
            };
            match flight.try_stop(self.turn.driver.driver()) {
                Ok(Outcome::Submitted | Outcome::Queued | Outcome::Completed) => {}
                Ok(Outcome::Deferred) => {
                    return Step::Done(Branch::Resume(next));
                }
                Err(cause) => {
                    engine.operations.recv.deny_cancel();
                    self.turn.driver.rejected(cause);
                    return Step::Done(Branch::Pending(Deferred::Recv));
                }
            }
            engine.operations.recv.cancel_sent();
            return Step::Done(Branch::Pending(Deferred::Recv));
        }
        match self
            .storage
            .entry
            .get()
            .engine
            .operations
            .lifecycle
            .close
            .owner
        {
            Ownership::Held => {
                match self.owner(Phase::Prepare) {
                    Step::Continue => {}
                    Step::Done(branch) => return Step::Done(branch),
                }
                match self.owner(Phase::Release) {
                    Step::Continue => {}
                    Step::Done(branch) => return Step::Done(branch),
                }
                self.storage
                    .entry
                    .get_mut()
                    .engine
                    .operations
                    .lifecycle
                    .close
                    .owner
                    .release();
            }
            Ownership::Released => {}
            Ownership::Claimed => return Step::Done(Branch::Stale),
        }
        match self.storage.egress.clear(
            self.storage.entry.get().key,
            self.turn.work,
            self.turn.driver,
        ) {
            egress::Clear::Empty => {}
            egress::Clear::Released { payload, .. } => {
                return Step::Done(Branch::Released {
                    payload,
                    resume: Resume::new(self.storage.entry.get()),
                });
            }
            egress::Clear::Retry => {
                return Step::Done(Branch::Resume(Resume::new(self.storage.entry.get())));
            }
            egress::Clear::Waiting => return Step::Done(Branch::Pending(Deferred::Egress)),
        }
        let slot = self.storage.entry.get();
        if slot.engine.operations.sending.is_inflight() {
            return Step::Done(Branch::Pending(Deferred::Send));
        }
        if slot.engine.establish.pending_fd().is_some() {
            return Step::Done(Branch::Pending(Deferred::Setup));
        }
        if slot.engine.operations.recv.is_armed() {
            return Step::Done(Branch::Pending(Deferred::Recv));
        }
        Step::Continue
    }

    fn owner(&mut self, phase: Phase) -> Step<'d, ID, R, B> {
        let step = (self.call.owner)((
            self.call.ctx,
            self.call.claim,
            phase,
            self.call.flights,
            self.storage.entry.get_mut(),
            self.turn.driver,
        ));
        match step {
            Decision::Runnable(next) => Step::Done(Branch::Resume(next)),
            Decision::External(wait) => {
                let target = self.storage.entry.get().engine.ready_handle().target();
                if wait.matches(self.storage.entry.get().key, target) {
                    Step::Done(Branch::Pending(Deferred::Owner(wait)))
                } else {
                    Step::Done(Branch::Stale)
                }
            }
            Decision::Stale => Step::Done(Branch::Stale),
            Decision::Ready => {
                if self.storage.entry.get().terminal.is_pending() {
                    Step::Done(Branch::Pending(Deferred::Terminal))
                } else {
                    Step::Continue
                }
            }
        }
    }

    pub(super) fn retire(self) -> Branch<'d, ID, R, B> {
        let Self {
            storage,
            turn,
            call,
        } = self;
        let result = storage.entry.try_remove(|mut slot| {
            let owner = match slot.control.engine.operations.lifecycle.close.owner.claim() {
                state::Claim::Ready(owner) => owner,
                state::Claim::Unavailable => return Err((slot, Branch::Stale)),
            };
            let (next, outcome) = slot.control.engine.establish.retire();
            slot.control.engine.establish = next;
            let (reclaim, handle) = match outcome {
                setup::Retire::Ready { close, handle } => (close, handle),
                setup::Retire::Pending(target) => {
                    slot.control
                        .engine
                        .operations
                        .lifecycle
                        .close
                        .owner
                        .restore(owner);
                    return Err((
                        slot,
                        Branch::Pending(Deferred::Retire(Resume::from_target(target))),
                    ));
                }
                setup::Retire::Unavailable => {
                    slot.control
                        .engine
                        .operations
                        .lifecycle
                        .close
                        .owner
                        .restore(owner);
                    return Err((slot, Branch::Stale));
                }
            };
            let result = (call.commit)((
                call.ctx,
                call.claim,
                slot.state,
                reclaim.kind(),
                turn.driver,
            ));
            let (state, branch) = match result {
                Approval::Ready(proof) => {
                    handle.set_target(slot.key.target().dispatch());
                    reclaim.close(turn.driver.driver());
                    return Ok((proof, slot.send.release()));
                }
                Approval::Pending(state) => {
                    (state, Branch::Resume(Resume::from_target(handle.target())))
                }
                Approval::Stale(state) => (state, Branch::Stale),
            };
            slot.state = state;
            slot.control.engine.establish = reclaim.restore();
            slot.control
                .engine
                .operations
                .lifecycle
                .close
                .owner
                .restore(owner);
            Err((slot, branch))
        });
        match result {
            Ok((proof, send)) => {
                if let Some(release) = <M::Ingress as Finish<W>>::release::<ID>(send) {
                    release.call(storage.runtime);
                }
                Branch::Retired(proof)
            }
            Err((_, branch)) => branch,
        }
    }
}
