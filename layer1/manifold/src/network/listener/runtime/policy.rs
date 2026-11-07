use std::{ops::ControlFlow, pin};

use dope_core::driver::{self, route, route::Identity, schedule};
use dope_net::{
    link::{
        egress::data::{self, compose, exec},
        event::{self, ParkError},
        pool::{
            self, pending,
            recv::{
                arrival, input,
                reserve::{Choice, Reservation},
            },
        },
    },
    wire::terminal,
};
use handler::{failures, policy::Policy};
use o3::base::cell::region;

use crate::{
    app::{
        env,
        receive::{self, ingress},
    },
    network::listener::{
        self, conn, handler,
        runtime::control,
        writer::{memory, phase},
    },
};

impl<'n, 'p, 's, 'w, 'd: 'w, ID: Identity + 'd, A, E> ingress::Policy<'n, 'w, 'd, ID>
    for listener::Listener<'n, 'p, 's, 'w, 'd, ID, A, E>
where
    A: handler::Application<'d, ID>,
    A::WireFailure: failures::Policy<'w, 'd, A, ID>,
    A::Input: Policy<'w, 'd, ID, A>,
    E: env::Spec<Wire = A::Wire>,
{
    type Transport = E::Transport;
    type Wire = A::Wire;
    type State = conn::State<'p, 's, 'd, ID, A::Conn, A::Egress>;
    type Payload = compose::Either<listener::raw::Owned<'p, 's, 'd, ID>, A::Egress>;
    type Reserve = <<A::Egress as data::raw::Payload<'d>>::Mode as exec::Mode>::Reserve<
        { memory::WRITE_BUF_CAP },
    >;
    type Input = A::Input;

    fn storage<'a>(self: pin::Pin<&'a mut Self>) -> &'a mut crate::Inbound<'n, 'w, 'd, ID, Self> {
        &mut *self.project().owner.pool_mut()
    }

    fn reserve<'a, R>(
        self: pin::Pin<&'a mut Self>,
        data: R,
        _: &region::Token<region::Identity<'d, 0>>,
    ) -> Reservation<'a, 'w, 'd, ID, Self::Wire, Self::Input>
    where
        R: arrival::Arrival<
                'd,
                input::Receipt<Self::Wire, Self::Input>,
                Observed = event::Data<'d>,
            >,
        input::Receipt<Self::Wire, Self::Input>: 'a,
        <Self::Input as input::Mode<Self::Wire>>::Reserve:
            Choice<Self::Wire, <Self::Input as input::Mode<Self::Wire>>::Ingress>,
    {
        self.project()
            .owner
            .pool_mut()
            .recv()
            .data(data, |_, _| Default::default())
    }

    fn admit(
        self: pin::Pin<&mut Self>,
        target: impl FnOnce() -> Option<route::Target<'d, ID>>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> ControlFlow<schedule::ready::Target<'d>> {
        control::admit_deadline(self, target, turn, driver)
    }

    fn receive<'input>(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        input: crate::Incoming<'input, 'w, 'd, Self::Wire, Self::Input>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> crate::Receiving<'w, 'd, Self::Wire, Self::Input> {
        <A::Input as Policy<'w, 'd, ID, A>>::receive(self, key, input, turn, driver)
    }

    fn finish(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        finish: ingress::Finish,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let wake = match finish {
            ingress::Finish::Empty(ingress::Empty::Discarded { finished }) => {
                if finished {
                    let mut this = self.as_mut().project();
                    let Some(mut egress) = this.owner.egress_mut(key) else {
                        return;
                    };
                    A::discarded(
                        this.app.as_mut(),
                        egress.context(turn.reborrow().app()),
                        driver,
                    );
                }
                phase::Wake::Progress
            }
            ingress::Finish::Empty(ingress::Empty::NoChunk) | ingress::Finish::Chunk => {
                phase::Wake::Ingress
            }
            ingress::Finish::CloseAfter => return phase::close_ready(self, key, turn, driver),
        };
        phase::commit_chunk(self, key, wake, turn, driver);
    }

    fn latch(mut self: pin::Pin<&mut Self>, key: pool::Key<'d, ID>) {
        let this = self.as_mut().project();
        if let Some(handle) = pending::Pending::of(this.owner.pool()).at(key) {
            handle.wake();
        }
    }

    fn terminal(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> ingress::Terminal {
        {
            let mut this = self.as_mut().project();
            let Some(mut egress) = this.owner.egress_mut(key) else {
                return ingress::Terminal::Idle;
            };
            let cause = match egress.connection.terminal.finish_recv() {
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
            <A::WireFailure as failures::Policy<'w, 'd, A, ID>>::wire_failed(
                this.app.as_mut(),
                egress.context(turn.reborrow().app()),
                cause,
            );
        }
        control::close_slot(self, key, turn, driver);
        ingress::Terminal::Closed
    }

    fn eof(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        end: receive::End,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        {
            let mut this = self.as_mut().project();
            let Some(mut egress) = this.owner.egress_mut(key) else {
                return;
            };
            egress.connection.end_discard();
            let mut conn = egress.context(turn.reborrow().app());
            conn.close_after();
            A::eof(this.app.as_mut(), conn, end, driver);
        }
        phase::commit_chunk(self, key, phase::Wake::Ingress, turn, driver);
    }

    fn close(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        cause: ingress::CloseCause,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        if let Some(slot) = self.as_mut().project().owner.pool_mut().get_mut(key) {
            match cause {
                ingress::CloseCause::Io(error) => slot.state.fail(conn::Failure::Receive(error)),
                ingress::CloseCause::Receive(ParkError::Submit(error)) => {
                    slot.state.fail(conn::Failure::Submit(error));
                }
                ingress::CloseCause::Receive(error) => slot.state.fail(conn::Failure::Park(error)),
                ingress::CloseCause::Application(error) => {
                    slot.state.fail(conn::Failure::Application(error));
                }
                ingress::CloseCause::Local | ingress::CloseCause::Transport => {}
            }
        }
        control::close_slot(self, key, turn, driver);
    }
}
