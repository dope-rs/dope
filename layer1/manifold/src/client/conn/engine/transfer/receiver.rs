use std::{convert, pin};

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

impl<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> app::Receiver<'w, 'd, ID, A>
    for pin::Pin<&mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>
where
    A: app::Receive<'d, ID> + app::Lifecycle<'w, 'd, ID>,
    A::Requests: send::Policy<'w, 'd, ID, A>,
    A::Continuation: liveness::Inspection<'d, ID, A>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: close::ClosePhase<'d, ID, A, S, E>,
    <A::Continuation as app::resume::Mode>::Outcome: effect::Effect<'w, 'd, ID, A, S, E, X>,
{
    fn borrowed<'input, 'turn, 'driver, 'context>(
        mut self,
        key: pool::Key<'d, ID>,
        batch: Items<'input, A::Wire>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut driver::Context<'context, 'd>,
    ) -> ingress::Progress<convert::Infallible>
    where
        'd: 'input,
        A: app::Borrowed<'d, ID>,
    {
        if let Some(slot) = self.as_mut().project().links.pool_mut().get_mut(key) {
            slot.state.record_recv(driver.driver_ref().clock().now());
        }
        batch::drive::<A::Wire, _>(batch, |chunk| {
            let outcome = {
                let this = self.as_mut().project();
                let Some(pool::EgressMut {
                    flights: _,
                    connection: slot,
                    pending: _,
                    queue: egress,
                }) = this.links.pool_mut().egress_mut(key)
                else {
                    return receive::Outcome::Continue;
                };

                match State::active(slot, |active, _| {
                    this.app.chunk(
                        conn::Ctx::new(active, turn.reborrow().app()),
                        egress.reborrow(),
                        chunk,
                        driver,
                    )
                }) {
                    flow::Active::Ready(callback) => callback,
                    flow::Active::Opening | flow::Active::Sealed => {
                        return receive::Outcome::CloseAfter;
                    }
                }
            };
            let (done, flow) = outcome.into_parts();
            done.apply(self.as_mut(), key, flow, turn.reborrow(), driver)
        })
    }

    fn retained<'turn, 'driver, 'context, R: Default>(
        mut self,
        key: pool::Key<'d, ID>,
        chunk: Chunk<'w, 'd, A::Wire>,
        receipt: input::Receipt<A::Wire, receive::Owned<R>>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut driver::Context<'context, 'd>,
    ) -> ingress::Progress<convert::Infallible>
    where
        A: app::Owned<'w, 'd, ID, R>,
    {
        let outcome = {
            let this = self.as_mut().project();
            let Some(pool::EgressMut {
                flights: _,
                connection: slot,
                pending: _,
                queue: egress,
            }) = this.links.pool_mut().egress_mut(key)
            else {
                return ingress::Progress::Done(receive::Outcome::Continue);
            };

            slot.state.record_recv(driver.driver_ref().clock().now());
            match State::active(slot, |active, _| {
                let receipt =
                    <<A::Wire as wire::Spec>::Receive as Policy<A::Wire>>::Publication::publish(
                        receipt,
                        || {
                            this.app
                                .receipt(active.id(), active.conn(), driver.region_token())
                        },
                    );
                this.app.chunk(
                    conn::Ctx::new(active, turn.reborrow().app()),
                    egress.reborrow(),
                    chunk,
                    receipt,
                    driver,
                )
            }) {
                flow::Active::Ready(callback) => callback,
                flow::Active::Opening | flow::Active::Sealed => {
                    return ingress::Progress::Done(receive::Outcome::CloseAfter);
                }
            }
        };
        let (done, next) = outcome.into_parts();
        ingress::Progress::Done(done.apply(self, key, next, turn, driver))
    }
}
