use std::pin;

use dope_core::driver::{retained, schedule};
use dope_net::link::pool;

use crate::{
    listener::{
        self, connection, handler,
        runtime::lifecycle,
        writer::{self, flush::Flush as _, phase::Phase as _, send},
    },
    receive::{self, ingress},
};
impl<'d, const ID: u8, A, E> ingress::Policy<'d, ID> for listener::Listener<'d, ID, A, E>
where
    A: handler::Application<'d, ID>,
    E: crate::Env<Wire = A::Wire>,
{
    type Transport = E::Transport;
    type Wire = A::Wire;
    type State = connection::State<'d, ID, A::Conn>;
    type Payload = writer::Payload<'d, ID>;
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
        self.project().owner.pool_mut()
    }

    fn receive<'input>(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        input: <Self::Input as receive::Delivery>::Value<'input, 'd, Self::Wire>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome {
        <A::Input as handler::Policy<'d, ID, A>>::receive(self, key, input, turn, driver)
    }

    fn finish(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        finish: ingress::Finish,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        match finish {
            ingress::Finish::Empty(empty) => {
                let refresh_idle = match empty {
                    ingress::Empty::NoChunk => false,
                    ingress::Empty::Discarded => true,
                };
                self.flush_after_recv(key, refresh_idle, turn, driver);
            }
            ingress::Finish::Chunk => {
                self.as_mut()
                    .flush_after_recv(key, false, turn.reborrow(), driver);
                send::SendPhase::arm_send_deadline(self, key, turn, driver);
            }
            ingress::Finish::CloseAfter => self.maybe_close_slot(key, turn, driver),
        }
    }

    fn close(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        _cause: ingress::CloseCause,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        lifecycle::Lifecycle::close_slot(self, key, turn, driver);
    }
}
