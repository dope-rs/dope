use std::pin;

use dope_core::driver::{retained, schedule};
use dope_net::link::pool;

use crate::{
    connector::{
        app, attempt, auxiliary,
        connection::{
            self,
            engine::{
                transfer::{access, send},
                transition::close,
            },
        },
        lifecycle,
    },
    receive::{self, ingress},
};

impl<'d, const ID: u8, A, S, E, X> ingress::Policy<'d, ID>
    for connection::Engine<'d, ID, A, S, E, X>
where
    A: app::Receive<'d, ID> + app::Lifecycle<'d, ID> + app::RequestSource<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: auxiliary::Mode<'d, A::Send, ID>,
{
    type Transport = E::Transport;
    type Wire = A::Wire;
    type State = connection::State<A::Conn, X::Owner>;
    type Payload = A::Send;
    type Input = A::Input;

    fn storage<'a>(
        self: pin::Pin<&'a mut Self>,
    ) -> &'a mut pool::Connections<
        'd,
        ID,
        Self::Transport,
        Self::Wire,
        Self::State,
        Self::Input,
        Self::Payload,
        { ingress::IOV_CAP },
    > {
        self.project().pool
    }

    fn receive<'input>(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        input: <Self::Input as receive::Delivery>::Value<'input, 'd, Self::Wire>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome {
        let receiver = access::Access::new(self);
        <A::Input as app::Policy<'d, ID, A>>::receive(receiver, key, input, turn, driver)
    }

    fn finish(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        finish: ingress::Finish,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        match finish {
            ingress::Finish::Empty(ingress::Empty::NoChunk | ingress::Empty::Discarded) => {
                if let Some(slot) = self.as_mut().project().pool.get_mut(key) {
                    slot.state.last_recv = Some(driver.turn_now());
                }
                send::SendPhase::submit_egress(self.as_mut(), key, turn.reborrow(), driver);
                close::ClosePhase::maybe_close(self, key, turn, driver);
            }
            ingress::Finish::Chunk | ingress::Finish::CloseAfter => {
                close::ClosePhase::maybe_close(self, key, turn, driver);
            }
        }
    }

    fn close(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        cause: ingress::CloseCause,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let reason = match cause {
            ingress::CloseCause::Local => lifecycle::CloseReason::Local,
            ingress::CloseCause::Transport => lifecycle::CloseReason::Transport,
            ingress::CloseCause::Capacity => lifecycle::CloseReason::Capacity,
            ingress::CloseCause::Protocol => lifecycle::CloseReason::Protocol,
            ingress::CloseCause::Remote => lifecycle::CloseReason::Remote,
        };
        close::ClosePhase::close_slot(self, key, reason, turn, driver);
    }
}
