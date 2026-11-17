use std::convert;

use dope_core::driver::{self, route::Identity, schedule};
use dope_net::{
    link::pool::{self, recv::input},
    wire::{
        self,
        receive::{Chunk, Policy, batch::Items, publication::Policy as _},
    },
};
use effect::Effect as _;

use crate::{
    app::{
        env,
        receive::{
            self,
            ingress::{self, batch},
        },
    },
    client::{
        app, attempt,
        conn::{
            self, State,
            engine::{
                self, liveness,
                ops::close,
                transfer::{effect, send},
            },
            flow,
        },
        side::mode::Mode,
    },
};

impl<'entry, 'context, 'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> app::Receiver<'w, 'd, ID, A>
    for (engine::Entry<'entry, 'p, 'w, 'd, ID, A, E::Transport>, &mut engine::Context<'context, 'd, ID, A, S, E, X>)
where
    A: app::Receive<'d, ID> + app::Lifecycle<'w, 'd, ID>,
    A::Requests: send::Policy<'w, 'd, ID, A>,
    A::Continuation: liveness::Inspection<'d, ID, A>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: close::ClosePhase<'p, 'w, 'd, ID, A, S, E, X>,
    <A::Continuation as app::resume::Mode>::Outcome: effect::Effect<'w, 'd, ID, A, S, E, X>,
{
    type Remaining = Option<engine::Entry<'entry, 'p, 'w, 'd, ID, A, E::Transport>>;

    fn borrowed<'input, 'turn, 'driver, 'ctx>(
        self,
        items: Items<'input, A::Wire>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut driver::Context<'ctx, 'd>,
    ) -> (Self::Remaining, ingress::Progress<convert::Infallible>)
    where
        'd: 'input,
        A: app::Borrowed<'d, ID>,
    {
        let (mut entry, context) = self;
        entry.get_mut().state.record_recv(driver.driver_ref().clock().now());
        batch::drive::<A::Wire, _, _>(items, entry, |mut entry, chunk| {
            let outcome = {
                let Some(pool::EgressMut { connection: slot, queue: egress, .. }) = entry.egress_mut()
                else { return (Some(entry), receive::Outcome::Continue) };
                match State::active(slot, |active, _| {
                    context.app.chunk(
                        conn::Ctx::new(active, turn.reborrow().app()),
                        egress.reborrow(), chunk, driver,
                    )
                }) {
                    flow::Active::Ready(callback) => callback,
                    flow::Active::Opening | flow::Active::Sealed => {
                        return (Some(entry), receive::Outcome::CloseAfter);
                    }
                }
            };
            let (done, flow) = outcome.into_parts();
            done.apply(context, entry, flow, turn.reborrow(), driver)
        })
    }

    fn retained<'turn, 'driver, 'ctx, R: Default>(
        self,
        chunk: Chunk<'w, 'd, A::Wire>,
        receipt: input::Receipt<A::Wire, receive::Owned<R>>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut driver::Context<'ctx, 'd>,
    ) -> (Self::Remaining, ingress::Progress<convert::Infallible>)
    where
        A: app::Owned<'w, 'd, ID, R>,
    {
        let (mut entry, context) = self;
        let outcome = {
            let Some(pool::EgressMut { connection: slot, queue: egress, .. }) = entry.egress_mut()
            else { return (Some(entry), ingress::Progress::Done(receive::Outcome::Continue)) };
            slot.state.record_recv(driver.driver_ref().clock().now());
            match State::active(slot, |active, _| {
                let receipt =
                    <<A::Wire as wire::Spec>::Receive as Policy<A::Wire>>::Publication::publish(
                        receipt,
                        || context.app.receipt(active.id(), active.conn(), driver.region_token()),
                    );
                context.app.chunk(
                    conn::Ctx::new(active, turn.reborrow().app()),
                    egress.reborrow(), chunk, receipt, driver,
                )
            }) {
                flow::Active::Ready(callback) => callback,
                flow::Active::Opening | flow::Active::Sealed => {
                    return (Some(entry), ingress::Progress::Done(receive::Outcome::CloseAfter));
                }
            }
        };
        let (done, next) = outcome.into_parts();
        let (entry, outcome) = done.apply(context, entry, next, turn, driver);
        (entry, ingress::Progress::Done(outcome))
    }
}
