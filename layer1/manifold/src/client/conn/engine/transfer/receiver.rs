use std::{convert, pin};

use dope_core::driver::{retained, route::Identity, schedule};
use dope_net::{
    link::pool,
    wire::receive::{Chunk, batch::Items},
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
                transfer::{effect, finish, send},
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
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    engine::Machine<'p, 'w, 'd, ID, A, S, E, X>:
        close::ClosePhase<'d, ID, A, S, E> + finish::Finish<'w, 'd, ID, A, A::Continuation>,
{
    fn borrowed<'input, 'turn, 'driver, 'context, 'owner>(
        mut self,
        key: pool::Key<'d, ID>,
        batch: Items<'input, A::Wire>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut retained::Context<'context, 'owner, 'd>,
    ) -> ingress::Progress<convert::Infallible>
    where
        'd: 'input,
        'd: 'owner,
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

                match State::active(slot, |active| {
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

    fn retained<'turn, 'driver, 'context, 'owner>(
        mut self,
        key: pool::Key<'d, ID>,
        chunk: Chunk<'w, 'd, A::Wire>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut retained::Context<'context, 'owner, 'd>,
    ) -> ingress::Progress<convert::Infallible>
    where
        'd: 'owner,
        A: app::Owned<'w, 'd, ID>,
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
            match State::active(slot, |active| {
                this.app.chunk(
                    conn::Ctx::new(active, turn.reborrow().app()),
                    egress.reborrow(),
                    chunk,
                    driver,
                )
            }) {
                flow::Active::Ready(callback) => callback,
                flow::Active::Opening | flow::Active::Sealed => {
                    return ingress::Progress::Done(receive::Outcome::CloseAfter);
                }
            }
        };
        <engine::Machine<'p, 'w, 'd, ID, A, S, E, X> as finish::Finish<
            'w,
            'd,
            ID,
            A,
            A::Continuation,
        >>::finish(self, key, outcome, turn, driver)
    }
}
