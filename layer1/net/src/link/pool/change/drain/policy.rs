use dope_core::driver::{
    life::finish, receive::credit, retained, route::Identity, schedule::maint,
};
use o3::base::{Call as _, cell::region};

use crate::{
    link::{
        egress::reserve::demand,
        pool::{
            self,
            change::{
                Finalize,
                close::{
                    Approval, Commit, Decision, Deferred, Failed, Handler, Outcome, Owner, Resume,
                    Start,
                },
                drain::{Branch, Call, Plan, Storage, Strategy, Turn, release},
                quiesce,
            },
            recv::{
                input,
                storage::{tail, tail::Slot},
            },
        },
        setup,
        slot::recv::Capture,
    },
    wire::{self, receive, send},
};

/// Statically selected teardown of connections owned by a pool.
pub trait Policy<W: wire::Spec>: Sized {
    fn close<'p, 'w, 'd, ID: Identity + 'w, T, S, M, B, const IOV: usize, D, R, C, X, F>(
        pool: &mut pool::Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
        key: pool::Key<'d, ID>,
        work: maint::Work<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
        handler: Handler<
            '_,
            C,
            X,
            impl for<'x, 'c, 'f, 's, 'v, 'r, 'o> FnMut(
                Owner<'x, 'c, 'f, 's, 'v, 'r, 'o, 'w, 'd, ID, W, S, C, X>,
            ) -> Decision<'d, ID>,
            impl for<'x, 'c, 'v, 'r, 'o> FnMut(
                Commit<'x, 'c, 'v, 'r, 'o, 'd, C, X, S>,
            ) -> Approval<S, R>,
            impl for<'x, 'c, 'i, 'v, 'r, 'o> FnMut(
                Failed<'x, 'c, 'i, 'v, 'r, 'o, 'p, 'w, 'd, ID, W, S, B, IOV, D, C, X>,
            ),
        >,
        release: impl release::Policy<'d, X, B>,
        complete: impl FnOnce(
            &mut pool::Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
            &mut X,
            &mut retained::Context<'_, '_, 'd>,
            Outcome<C::Output, C, Deferred<'d, ID>>,
        ) -> F,
    ) -> F
    where
        'd: 'w,
        T: crate::Transport,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = Self>,
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
        D: demand::Mode,
        F: FnMut(B, &mut region::Token<region::Identity<'d, 1>>);

    fn finish_keyed<'p, 'w, 'd, ID: Identity + 'w, T, S, M, B, const IOV: usize, D, F>(
        pool: &mut pool::Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
        finish: &mut finish::Finish<'_, 'd>,
        free: F,
    ) where
        'd: 'w,
        T: crate::Transport,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = Self>,
        D: demand::Mode,
        F: FnMut(pool::Key<'d, ID>, B);
}

impl<W, I> Policy<W> for I
where
    W: wire::Spec,
    I: Strategy<W> + quiesce::Strategy<W>,
    <W::Receive as receive::Policy<W>>::Eof: Capture<W>,
{
    fn close<'p, 'w, 'd, ID: Identity + 'w, T, S, M, B, const IOV: usize, D, R, C, X, F>(
        pool: &mut pool::Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
        key: pool::Key<'d, ID>,
        work: maint::Work<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
        handler: Handler<
            '_,
            C,
            X,
            impl for<'x, 'c, 'f, 's, 'v, 'r, 'o> FnMut(
                Owner<'x, 'c, 'f, 's, 'v, 'r, 'o, 'w, 'd, ID, W, S, C, X>,
            ) -> Decision<'d, ID>,
            impl for<'x, 'c, 'v, 'r, 'o> FnMut(
                Commit<'x, 'c, 'v, 'r, 'o, 'd, C, X, S>,
            ) -> Approval<S, R>,
            impl for<'x, 'c, 'i, 'v, 'r, 'o> FnMut(
                Failed<'x, 'c, 'i, 'v, 'r, 'o, 'p, 'w, 'd, ID, W, S, B, IOV, D, C, X>,
            ),
        >,
        mut release: impl release::Policy<'d, X, B>,
        complete: impl FnOnce(
            &mut pool::Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
            &mut X,
            &mut retained::Context<'_, '_, 'd>,
            Outcome<C::Output, C, Deferred<'d, ID>>,
        ) -> F,
    ) -> F
    where
        'd: 'w,
        T: crate::Transport,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = Self>,
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
        let branch = 'close: {
            let Some((entry, storage)) =
                Slot::receive::<M::Ingress>(&mut pool.prepared.slab, key.parts())
            else {
                break 'close Branch::Missing;
            };
            let resident = match Start::begin(entry) {
                Start::Ready(resident) => resident,
                Start::Terminal => break 'close Branch::Pending(Deferred::Terminal),
            };
            let target = resident.get().key.target();
            let id = credit::Id::new(target.dispatch());
            if let Some(cancel) = <Self as Strategy<W>>::cancel(&storage.storage) {
                cancel.call((&mut pool.prepared.runtime, id));
            }
            let mut entry = tail::Store::clear(resident);
            let stop = entry.get_mut().control.engine.cancel(driver.driver());
            match stop {
                setup::Cancellation::Ready => {}
                setup::Cancellation::Pending => {
                    break 'close Branch::Pending(Deferred::Setup);
                }
                setup::Cancellation::Retry => {
                    let next = Resume::new(entry.get());
                    break 'close Branch::Resume(next);
                }
                setup::Cancellation::Rejected(cause) => {
                    driver.rejected(cause);
                    break 'close Branch::Pending(Deferred::Setup);
                }
            }
            let plan = Plan {
                storage: Storage::<ID, W, S, M, B, IOV, D> {
                    entry,
                    egress: &mut pool.prepared.egress,
                    runtime: &mut pool.prepared.runtime,
                    recv: storage,
                },
                turn: Turn {
                    target,
                    work,
                    driver,
                },
                call: Call {
                    flights: &pool.prepared.flights,
                    claim: &claim,
                    ctx,
                    owner: &mut owner,
                    commit: &mut commit,
                    failed: &mut failed,
                },
            };
            <Self as Strategy<W>>::close(plan)
        };
        if let Some(handle) = pool.prepared.pending.handle(key) {
            let waiting = pool
                .prepared
                .slab
                .view()
                .get(key.parts())
                .is_some_and(|slot| slot.send_status().is_waiting());
            if !waiting || matches!(&branch, Branch::Pending(Deferred::Owner(_))) {
                handle.wait_send(waiting);
            }
        }
        if pool.prepared.pending.has_waiting()
            && <W::Send as send::Policy<W>>::has_capacity(&pool.prepared.runtime)
        {
            pool.prepared.pending.release_send();
        }
        let outcome = match branch {
            Branch::Retired(proof) => Outcome::Retired(claim.finalize(proof)),
            Branch::Pending(deferred) => Outcome::Pending(claim, deferred),
            Branch::Resume(next) => {
                next.reschedule();
                Outcome::Pending(claim, Deferred::Scheduled)
            }
            Branch::Released { payload, resume } => {
                release.release(ctx, payload, driver);
                resume.reschedule();
                Outcome::Pending(claim, Deferred::Scheduled)
            }
            Branch::Stale => Outcome::Stale(claim),
            Branch::Missing => Outcome::Missing(claim),
        };
        complete(pool, ctx, driver, outcome)
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
        D: demand::Mode,
        F: FnMut(B, &mut region::Token<region::Identity<'d, 1>>),
    {
        quiesce::Plan::new(&mut pool.prepared).run(finish, free);
    }

    fn finish_keyed<'p, 'w, 'd, ID: Identity + 'w, T, S, M, B, const IOV: usize, D, F>(
        pool: &mut pool::Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
        finish: &mut finish::Finish<'_, 'd>,
        free: F,
    ) where
        'd: 'w,
        T: crate::Transport,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = Self>,
        D: demand::Mode,
        F: FnMut(pool::Key<'d, ID>, B),
    {
        quiesce::Plan::new(&mut pool.prepared).keyed(finish, free);
    }
}
