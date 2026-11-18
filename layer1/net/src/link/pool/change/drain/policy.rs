use dope_core::driver::{self, life::finish, receive::credit, route::Identity, schedule::maint};
use o3::{base::cell::region, depot::raw::Proof};

use crate::{
    link::{
        egress::{data, reserve::demand},
        pool::{
            self,
            change::{
                Finalize,
                close::{
                    Approval, Commit, Decision, Failed, Handler, Outcome, Owner, Resume, Start,
                },
                drain::{Branch, Call, Plan, Storage, Strategy, Turn},
            },
            pending,
            recv::{self, input, storage::tail},
        },
        setup,
        slot::recv::Capture,
    },
    wire::{self, receive, send},
};

/// Statically selected teardown of connections owned by a pool.
pub trait Policy<W: wire::Spec>: Sized {
    fn close<'a, 'p, 'w, 'd, ID: Identity + 'w, T, S, M, B, const IOV: usize, D, R, C, X>(
        entry: Result<
            pool::Entry<'a, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
            pool::Missing<'a, 'p, 'w, 'd, ID, W>,
        >,
        work: maint::Work<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
        handler: Handler<
            '_,
            C,
            X,
            impl for<'x, 'c, 'f, 's, 'v, 'r> FnMut(
                Owner<'x, 'c, 'f, 's, 'v, 'r, 'w, 'd, ID, W, S, C, X>,
            ) -> Decision<'d, ID>,
            impl for<'x, 'c, 'v, 'r> FnMut(Commit<'x, 'c, 'v, 'r, 'd, C, X, S>) -> Approval<S, R>,
            impl for<'x, 'c, 'i, 'v, 'r> FnMut(
                Failed<'x, 'c, 'i, 'v, 'r, 'p, 'w, 'd, ID, W, S, B, IOV, D, C, X>,
            ),
        >,
        release: impl FnMut(&mut X, B, &mut region::Token<region::Identity<'d, 1>>),
    ) -> Outcome<C::Output, C, pool::Entry<'a, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>>
    where
        'd: 'w,
        T: crate::Transport,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = Self>,
        B: data::raw::Payload<'d>,
        D: demand::Mode,
        C: Finalize<R>;

    fn finish<'p, 'w, 'd, ID: Identity + 'w, T, S, M, B, const IOV: usize, D, F>(
        pool: &mut pool::Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
        finish: &mut finish::Finish<'_, 'd>,
        free: F,
    ) where
        'd: 'w,
        T: crate::Transport,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = Self>,
        B: data::raw::Payload<'d>,
        D: demand::Mode,
        F: FnMut(B, &mut region::Token<region::Identity<'d, 1>>);

    fn finish_keyed<'p, 'w, 'd, ID: Identity + 'w, T, S, M, B, const IOV: usize, D, R, F>(
        pool: &mut pool::Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
        finish: &mut finish::Finish<'_, 'd>,
        retire: impl FnMut(&mut S) -> R,
        free: F,
    ) where
        'd: 'w,
        T: crate::Transport,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = Self>,
        B: data::raw::Payload<'d>,
        D: demand::Mode,
        F: FnMut(pool::Key<'d, ID>, pool::Released<R, B>);
}

impl<W, I> Policy<W> for I
where
    W: wire::Spec,
    I: Strategy<W>,
    <W::Receive as receive::Policy<W>>::Eof: Capture<W>,
{
    fn close<'a, 'p, 'w, 'd, ID: Identity + 'w, T, S, M, B, const IOV: usize, D, R, C, X>(
        entry: Result<
            pool::Entry<'a, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
            pool::Missing<'a, 'p, 'w, 'd, ID, W>,
        >,
        work: maint::Work<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
        handler: Handler<
            '_,
            C,
            X,
            impl for<'x, 'c, 'f, 's, 'v, 'r> FnMut(
                Owner<'x, 'c, 'f, 's, 'v, 'r, 'w, 'd, ID, W, S, C, X>,
            ) -> Decision<'d, ID>,
            impl for<'x, 'c, 'v, 'r> FnMut(Commit<'x, 'c, 'v, 'r, 'd, C, X, S>) -> Approval<S, R>,
            impl for<'x, 'c, 'i, 'v, 'r> FnMut(
                Failed<'x, 'c, 'i, 'v, 'r, 'p, 'w, 'd, ID, W, S, B, IOV, D, C, X>,
            ),
        >,
        mut release: impl FnMut(&mut X, B, &mut region::Token<region::Identity<'d, 1>>),
    ) -> Outcome<C::Output, C, pool::Entry<'a, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>>
    where
        'd: 'w,
        T: crate::Transport,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = Self>,
        B: data::raw::Payload<'d>,
        D: demand::Mode,
        C: Finalize<R>,
    {
        let Handler {
            claim,
            context: ctx,
            mut owner,
            mut commit,
            mut failed,
        } = handler;
        let entry = match entry {
            Ok(entry) => entry,
            Err(pool::Missing {
                key,
                runtime,
                pending,
            }) => {
                settle::<ID, W>(pending, runtime, key, false, false);
                return Outcome::Missing(claim);
            }
        };
        let key = entry.sender.key;
        let pool::change::raw::Prepared {
            slab,
            egress,
            runtime,
            flights,
            pending,
            ..
        } = &mut *entry.sender.root;
        // SAFETY: consumption retains the original initialized slot and all paired owners.
        let (slot, recv) = tail::Slot::from_live(unsafe { Proof::new((slab, key.parts().slot())) });
        let (branch, waiting) = 'close: {
            let resident = match Start::begin(slot) {
                Start::Ready(resident) => resident,
                Start::Terminal(waiting) => break 'close (Branch::Pending, waiting),
            };
            // SAFETY: selection retains the resident and its paired receive storage.
            let storage: recv::Borrow<'_, 'd, ID, M::Ingress, input::Receipt<W, M>> =
                unsafe { Proof::new((recv, key)) }.into();
            let target = resident.get().key.target();
            let id = credit::Id::new(target.dispatch());
            <Self as Strategy<W>>::cancel::<ID, input::Receipt<W, M>>(
                &storage.storage,
                runtime,
                id,
            );
            let mut entry = tail::Store::clear(resident);
            let stop = entry.get_mut().control.engine.cancel(driver);
            match stop {
                setup::Cancellation::Ready => {}
                setup::Cancellation::Pending => {
                    break 'close (Branch::Pending, entry.get().send_status().is_waiting());
                }
                setup::Cancellation::Retry => {
                    let next = Resume::new(entry.get());
                    break 'close (Branch::Resume(next), entry.get().send_status().is_waiting());
                }
                setup::Cancellation::Rejected(cause) => {
                    driver.rejected(cause);
                    break 'close (Branch::Pending, entry.get().send_status().is_waiting());
                }
            }
            let plan = Plan {
                storage: Storage::<ID, W, S, M, B, IOV, D> {
                    entry,
                    egress,
                    runtime: &mut *runtime,
                    recv: storage,
                },
                turn: Turn {
                    target,
                    work,
                    driver,
                },
                call: Call {
                    flights,
                    claim: &claim,
                    ctx,
                    owner: &mut owner,
                    commit: &mut commit,
                    failed: &mut failed,
                },
            };
            <Self as Strategy<W>>::close(plan)
        };
        let waiting = settle::<ID, W>(
            pending,
            runtime,
            key,
            waiting,
            matches!(&branch, Branch::Owner(_)),
        );
        match branch {
            Branch::Retired(proof) => Outcome::Retired(claim.finalize(proof)),
            Branch::Pending => Outcome::Pending(entry, claim),
            Branch::Owner(wait) => {
                if !waiting {
                    wait.reschedule();
                }
                Outcome::Pending(entry, claim)
            }
            Branch::Resume(next) => {
                next.reschedule();
                Outcome::Pending(entry, claim)
            }
            Branch::Released { payload, resume } => {
                release(ctx, payload, driver.state_token());
                resume.reschedule();
                Outcome::Pending(entry, claim)
            }
            Branch::Stale => Outcome::Stale(entry, claim),
        }
    }

    fn finish<'p, 'w, 'd, ID: Identity + 'w, T, S, M, B, const IOV: usize, D, F>(
        pool: &mut pool::Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
        finish: &mut finish::Finish<'_, 'd>,
        free: F,
    ) where
        'd: 'w,
        T: crate::Transport,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = Self>,
        B: data::raw::Payload<'d>,
        D: demand::Mode,
        F: FnMut(B, &mut region::Token<region::Identity<'d, 1>>),
    {
        pool.prepared.run(finish, free);
    }

    fn finish_keyed<'p, 'w, 'd, ID: Identity + 'w, T, S, M, B, const IOV: usize, D, R, F>(
        pool: &mut pool::Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
        finish: &mut finish::Finish<'_, 'd>,
        retire: impl FnMut(&mut S) -> R,
        free: F,
    ) where
        'd: 'w,
        T: crate::Transport,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = Self>,
        B: data::raw::Payload<'d>,
        D: demand::Mode,
        F: FnMut(pool::Key<'d, ID>, pool::Released<R, B>),
    {
        pool.prepared.keyed(finish, retire, free);
    }
}

fn settle<'w, 'd: 'w, ID: Identity + 'w, W: wire::Spec + 'w + 'd>(
    pending: &pending::Queue<'d, ID>,
    runtime: &W::Runtime<'w, 'd, ID>,
    key: pool::Key<'d, ID>,
    waiting: bool,
    owner: bool,
) -> bool {
    let waiting = if let Some(handle) = pending.handle(key) {
        if !waiting || owner {
            handle.wait_send(waiting);
        }
        waiting
    } else {
        false
    };
    if pending.has_waiting() && <W::Send as send::Policy<W>>::has_capacity(runtime) {
        pending.release_send();
    }
    waiting
}
