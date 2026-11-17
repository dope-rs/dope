use std::pin;

use dope_core::driver::{self, route::Identity, schedule};
use dope_net::{
    link::{
        egress::reserve::demand,
        event::ParkError,
        pool::{
            self,
            recv::input,
        },
    },
    wire::terminal,
};
use engine::ops::close::{self, ClosePhase as _};
use o3::base::cell::region;

use crate::{
    app::{
        env,
        receive::{
            self,
            ingress::{self, Dispatch},
        },
    },
    client::{
        app, attempt,
        close::{Directive, LocalOrigin, Reason},
        conn::{
            self, State,
            engine::{
                self, liveness,
                transfer::{effect, send},
            },
            flow,
        },
        side,
        side::mode,
    },
};

impl<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> ingress::Policy<'p, 'w, 'd, ID>
    for engine::Machine<'p, 'w, 'd, ID, A, S, E, X>
where
    A: app::Receive<'d, ID> + app::Lifecycle<'w, 'd, ID>,
    A::Input: app::Policy<'w, 'd, ID, A> + Dispatch<A::Wire>,
    A::Requests: send::Policy<'w, 'd, ID, A>,
    A::Continuation: liveness::Inspection<'d, ID, A>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: mode::Mode<'d, ID>
        + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    Self: close::ClosePhase<'p, 'w, 'd, ID, A, S, E, X>,
    <A::Continuation as app::resume::Mode>::Outcome: effect::Effect<'w, 'd, ID, A, S, E, X>,
{
    type Transport = E::Transport;
    type Wire = A::Wire;
    type State = conn::State<'d, A::Conn, side::Primary<'d, ID>>;
    type Payload = A::Send;
    type Reserve = demand::Fallible;
    type Input = A::Input;

    type Context<'a> = engine::Context<'a, 'd, ID, A, S, E, X> where Self: 'a;

    fn parts<'a>(
        self: pin::Pin<&'a mut Self>,
    ) -> (&'a mut crate::Inbound<'p, 'w, 'd, ID, Self>, Self::Context<'a>) {
        let (pool, context) = engine::Machine::parts(self);
        (pool, context)
    }

    fn receipt<'ctx>(
        context: &mut Self::Context<'ctx>,
        key: pool::Key<'d, ID>,
        state: &Self::State,
        token: &region::Token<region::Identity<'d>>,
    ) -> <Self::Input as input::Mode<Self::Wire>>::Receipt
    where
        Self: 'ctx,
    {
        match state.conn() {
            Some(conn) => context.app.receipt(conn::Id::from_key(key), conn, token),
            None => Default::default(),
        }
    }

    fn receive<'ctx, 'a, 'input>(
        context: &mut Self::Context<'ctx>,
        entry: ingress::Entry<'a, 'p, 'w, 'd, ID, Self>,
        input: crate::Incoming<'input, 'w, 'd, Self::Wire, Self::Input>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> (Option<ingress::Entry<'a, 'p, 'w, 'd, ID, Self>>, crate::Receiving<'w, 'd, Self::Wire, Self::Input>)
    where
        Self: 'ctx,
    {
        <A::Input as app::Policy<'w, 'd, ID, A>>::receive((entry, context), input, turn, driver)
    }

    fn finish<'ctx, 'a>(
        context: &mut Self::Context<'ctx>,
        mut entry: ingress::Entry<'a, 'p, 'w, 'd, ID, Self>,
        finish: ingress::Finish,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Option<ingress::Entry<'a, 'p, 'w, 'd, ID, Self>>
    where
        Self: 'ctx,
    {
        match finish {
            ingress::Finish::Empty(ingress::Empty::NoChunk | ingress::Empty::Discarded { .. }) => {
                entry.get_mut().state.record_recv(driver.driver_ref().clock().now());
                <A::Requests as send::Policy<'w, 'd, ID, A>>::submit_entry::<false, false, S, E, X>(
                    context, entry, turn, driver,
                )
            }
            ingress::Finish::Chunk | ingress::Finish::CloseAfter => {
                Self::maybe_entry(context, entry, turn, driver)
            }
        }
    }

    fn terminal<'ctx, 'a>(
        context: &mut Self::Context<'ctx>,
        mut entry: ingress::Entry<'a, 'p, 'w, 'd, ID, Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> (Option<ingress::Entry<'a, 'p, 'w, 'd, ID, Self>>, ingress::Terminal)
    where
        Self: 'ctx,
    {
        {
            let Some(pool::EgressMut { connection: slot, queue, .. }) = entry.egress_mut()
            else { return (Some(entry), ingress::Terminal::Idle) };
            if slot.terminal.is_pending() && slot.is_app_held() {
                return (Some(entry), ingress::Terminal::Pending);
            }
            let cause = match slot.terminal.finish_recv() {
                terminal::Receive::Idle => return (Some(entry), ingress::Terminal::Idle),
                terminal::Receive::Pending => return (Some(entry), ingress::Terminal::Pending),
                terminal::Receive::Eof => {
                    return (Some(entry), ingress::Terminal::Ended(receive::End::Read));
                }
                terminal::Receive::Ready(terminal::Terminal::Clean) => {
                    return (Some(entry), ingress::Terminal::Ended(receive::End::Both));
                }
                terminal::Receive::Ready(terminal::Terminal::Failed(cause)) => cause,
            };
            slot.state.closing.failed(Reason::Transport);
            match State::terminal(slot, |conn| {
                context.app.wire_fail(conn, queue.reborrow(), cause, driver);
            }) {
                flow::Active::Ready(flow::Flow::Continue(()))
                | flow::Active::Ready(flow::Flow::Discarded(()))
                | flow::Active::Opening => {}
                flow::Active::Sealed => return (Some(entry), ingress::Terminal::Closed),
            }
        }
        (
            Self::close_entry(context, entry, Reason::Transport, turn, driver),
            ingress::Terminal::Closed,
        )
    }

    fn eof<'ctx, 'a>(
        context: &mut Self::Context<'ctx>,
        mut entry: ingress::Entry<'a, 'p, 'w, 'd, ID, Self>,
        end: receive::End,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Option<ingress::Entry<'a, 'p, 'w, 'd, ID, Self>>
    where
        Self: 'ctx,
    {
        let (eof, reason) = {
            let Some(pool::EgressMut { connection: slot, queue, .. }) = entry.egress_mut()
            else { return Some(entry) };
            let reason = slot.state.reconnect(Reason::Remote).reason();
            slot.close_after();
            let flow = State::active(slot, |active, _| {
                app::Receive::eof(
                    context.app, conn::Ctx::new(active, turn.reborrow().app()),
                    end, queue.reborrow(), driver,
                )
            });
            let eof = match flow {
                flow::Active::Ready(flow) => flow,
                flow::Active::Opening => flow::Flow::Continue(app::Eof::Close),
                flow::Active::Sealed => {
                    return Self::close_entry(context, entry, reason, turn, driver);
                }
            };
            (eof, reason)
        };
        match eof {
            flow::Flow::Continue(app::Eof::Retain) => {
                <A::Requests as send::Policy<'w, 'd, ID, A>>::submit_entry::<false, false, S, E, X>(
                    context, entry, turn, driver,
                )
            }
            flow::Flow::Discarded(app::Eof::Close | app::Eof::Retain)
            | flow::Flow::Continue(app::Eof::Close) => {
                Self::close_entry(context, entry, reason, turn, driver)
            }
        }
    }

    fn close<'ctx, 'a>(
        context: &mut Self::Context<'ctx>,
        mut entry: ingress::Entry<'a, 'p, 'w, 'd, ID, Self>,
        cause: ingress::CloseCause,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Option<ingress::Entry<'a, 'p, 'w, 'd, ID, Self>>
    where
        Self: 'ctx,
    {
        let reason = match cause {
            ingress::CloseCause::Local => Reason::Local(LocalOrigin::OperationCancelled),
            ingress::CloseCause::Transport => Reason::Transport,
            ingress::CloseCause::Io(error) => {
                let Some(pool::EgressMut { connection: slot, queue, .. }) = entry.egress_mut()
                else { return Some(entry) };
                slot.state.closing.apply(Directive::reconnect(Reason::Transport));
                match State::terminal(slot, |conn| {
                    context.app.io_fail(conn, queue.reborrow(), error, driver);
                }) {
                    flow::Active::Ready(flow::Flow::Continue(()))
                    | flow::Active::Ready(flow::Flow::Discarded(()))
                    | flow::Active::Opening => {}
                    flow::Active::Sealed => return Some(entry),
                }
                Reason::Transport
            }
            ingress::CloseCause::Application(err) => err.into(),
            ingress::CloseCause::Receive(error) => {
                let slot = entry.get_mut();
                let error = match error {
                    ParkError::Submit(error) => {
                        ParkError::Submit(context.controller.rejected(slot.state.owner.attempt(), error))
                    }
                    error => error,
                };
                let rule = Directive::from_receive(error);
                slot.state.closing.apply(rule);
                rule.reason()
            }
        };
        Self::close_entry(context, entry, reason, turn, driver)
    }
}
