use std::pin;

use dope_core::driver::{retained, route::Identity, schedule};
use dope_net::{
    link::{
        egress::reserve::demand,
        event::ParkError,
        pool::{self, pending},
    },
    wire::terminal,
};
use engine::ops::close::{self, ClosePhase as _};

use crate::{
    app::{
        env,
        receive::{
            self, Mode,
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
                transfer::{finish, send},
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
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: mode::Mode<'d, ID>
        + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    Self: close::ClosePhase<'d, ID, A, S, E> + finish::Finish<'w, 'd, ID, A, A::Continuation>,
{
    type Transport = E::Transport;
    type Wire = A::Wire;
    type State = conn::State<'d, A::Conn, side::Primary<'d, ID>>;
    type Payload = A::Send;
    type Reserve = demand::Fallible;
    type Input = A::Input;

    fn storage<'a>(self: pin::Pin<&'a mut Self>) -> &'a mut crate::Inbound<'p, 'w, 'd, ID, Self> {
        self.project().links.pool_mut()
    }

    fn receive<'input>(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        input: <Self::Input as Mode<Self::Wire>>::Value<'input, 'w, 'd>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Receiving<'w, 'd, Self::Wire, Self::Input> {
        <A::Input as app::Policy<'w, 'd, ID, A>>::receive(self, key, input, turn, driver)
    }

    fn finish(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        finish: ingress::Finish,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        match finish {
            ingress::Finish::Empty(ingress::Empty::NoChunk | ingress::Empty::Discarded { .. }) => {
                if let Some(slot) = self.as_mut().project().links.pool_mut().get_mut(key) {
                    slot.state.record_recv(driver.driver_ref().clock().now());
                }
                <A::Requests as send::Policy<'w, 'd, ID, A>>::submit(
                    self.as_mut(),
                    key,
                    turn.reborrow(),
                    driver,
                );
                self.maybe_close(key, turn, driver);
            }
            ingress::Finish::Chunk | ingress::Finish::CloseAfter => {
                self.maybe_close(key, turn, driver);
            }
        }
    }

    fn latch(mut self: pin::Pin<&mut Self>, key: pool::Key<'d, ID>) {
        let this = self.as_mut().project();
        if let Some((slot, handle)) = pending::Mut::of(this.links.pool_mut()).get(key) {
            if let Some(kind) = slot.terminal.kind() {
                slot.state.observe(kind);
            }
            handle.wake();
        }
    }

    fn terminal(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> ingress::Terminal {
        {
            let this = self.as_mut().project();
            let Some(pool::EgressMut {
                flights: _,
                connection: slot,
                pending: _,
                queue,
            }) = this.links.pool_mut().egress_mut(key)
            else {
                return ingress::Terminal::Idle;
            };
            if slot.terminal.has_eof() && slot.is_app_held() {
                return ingress::Terminal::Pending;
            }
            let cause = match slot.terminal.finish_recv() {
                terminal::Receive::Idle => return ingress::Terminal::Idle,
                terminal::Receive::Pending => return ingress::Terminal::Pending,
                terminal::Receive::Eof => {
                    return ingress::Terminal::Ended(receive::End::Read);
                }
                terminal::Receive::Ready(terminal::Terminal::Clean) => {
                    return ingress::Terminal::Ended(receive::End::Both);
                }
                terminal::Receive::Ready(terminal::Terminal::Failed(cause)) => cause,
            };
            slot.state.observe(terminal::Kind::Failed);
            let flow = State::terminal(slot, |conn| {
                this.app.wire_fail(conn, queue.reborrow(), cause, driver);
            });
            match flow {
                flow::Active::Ready(flow::Flow::Continue(()))
                | flow::Active::Ready(flow::Flow::Discarded(()))
                | flow::Active::Opening => {}
                flow::Active::Sealed => return ingress::Terminal::Closed,
            }
        }
        self.close_slot(key, Reason::Protocol, turn, driver);
        ingress::Terminal::Closed
    }

    fn eof(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        end: receive::End,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let (eof, reason) = {
            let this = self.as_mut().project();
            let Some(pool::EgressMut {
                flights: _,
                connection: slot,
                pending: _,
                queue,
            }) = this.links.pool_mut().egress_mut(key)
            else {
                return;
            };
            let reason = slot.state.reconnect(Reason::Remote).reason();
            slot.close_after();
            let flow = State::active(slot, |active| {
                app::Receive::eof(
                    this.app,
                    conn::Ctx::new(active, turn.reborrow().app()),
                    end,
                    queue.reborrow(),
                    driver,
                )
            });
            let eof = match flow {
                flow::Active::Ready(flow) => flow,
                flow::Active::Opening => flow::Flow::Continue(app::Eof::Close),
                flow::Active::Sealed => {
                    return self.close_slot(key, reason, turn, driver);
                }
            };
            (eof, reason)
        };
        match eof {
            flow::Flow::Continue(app::Eof::Retain) => {
                <A::Requests as send::Policy<'w, 'd, ID, A>>::submit(
                    self.as_mut(),
                    key,
                    turn.reborrow(),
                    driver,
                );
                return;
            }
            flow::Flow::Discarded(app::Eof::Close | app::Eof::Retain)
            | flow::Flow::Continue(app::Eof::Close) => {}
        }
        self.close_slot(key, reason, turn, driver);
    }

    fn close(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        cause: ingress::CloseCause,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let reason = match cause {
            ingress::CloseCause::Local => Reason::Local(LocalOrigin::OperationCancelled),
            ingress::CloseCause::Transport => Reason::Transport,
            ingress::CloseCause::Io(error) => {
                {
                    let this = self.as_mut().project();
                    let Some(pool::EgressMut {
                        flights: _,
                        connection: slot,
                        pending: _,
                        queue,
                    }) = this.links.pool_mut().egress_mut(key)
                    else {
                        return;
                    };
                    slot.state
                        .closing
                        .apply(Directive::reconnect(Reason::Transport));
                    let flow = State::terminal(slot, |conn| {
                        this.app.io_fail(conn, queue.reborrow(), error, driver);
                    });
                    match flow {
                        flow::Active::Ready(flow::Flow::Continue(()))
                        | flow::Active::Ready(flow::Flow::Discarded(()))
                        | flow::Active::Opening => {}
                        flow::Active::Sealed => return,
                    }
                }
                self.close_slot(key, Reason::Transport, turn, driver);
                return;
            }
            ingress::CloseCause::Application(err) => err.into(),
            ingress::CloseCause::Receive(error) => {
                let this = self.as_mut().project();
                let Some(slot) = this.links.pool_mut().get_mut(key) else {
                    return;
                };
                let error = match error {
                    ParkError::Submit(error) => ParkError::Submit(
                        this.controller.rejected(slot.state.owner.attempt(), error),
                    ),
                    error => error,
                };
                let rule = Directive::from_receive(error);
                slot.state.closing.apply(rule);
                rule.reason()
            }
        };
        self.close_slot(key, reason, turn, driver);
    }
}
