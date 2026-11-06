use std::{ops, pin};

use dope_core::{
    driver::{self, route, route::Identity, schedule},
    io::event::net::recv,
};
use dope_net::{
    link::{
        egress::reserve::demand,
        event,
        pool::{
            self,
            recv::{
                arrival, input,
                reserve::{Choice, Reservation},
            },
        },
    },
    wire,
};
use o3::base::cell::region;

use crate::app::receive::{
    End, Mode,
    ingress::{self, Admission as _, Dispatch, Strategy},
};

/// Shared receive state machine; endpoint lifecycle stays with its implementor.
pub(crate) trait Policy<'p, 'w, 'd: 'w + 'p, ID: Identity + 'd>: Sized {
    type Transport: dope_net::Transport;
    type Wire: wire::Spec + 'w + 'd;
    type State;
    type Payload;
    type Reserve: demand::Mode;
    type Input: Mode<Self::Wire> + Dispatch<Self::Wire>;

    fn storage<'a>(self: pin::Pin<&'a mut Self>) -> &'a mut crate::Inbound<'p, 'w, 'd, ID, Self>;

    fn reserve<'a, A>(
        self: pin::Pin<&'a mut Self>,
        data: A,
        token: &region::Token<region::Identity<'d, 0>>,
    ) -> Reservation<'a, 'w, 'd, ID, Self::Wire, Self::Input>
    where
        A: arrival::Arrival<
                'd,
                input::Receipt<Self::Wire, Self::Input>,
                Observed = event::Data<'d>,
            >,
        input::Receipt<Self::Wire, Self::Input>: 'a,
        <Self::Input as input::Mode<Self::Wire>>::Reserve:
            Choice<Self::Wire, <Self::Input as input::Mode<Self::Wire>>::Ingress>;

    /// Admits input before its native completion or held tail is consumed.
    fn admit(
        self: pin::Pin<&mut Self>,
        _: impl FnOnce() -> Option<route::Target<'d, ID>>,
        _: schedule::Turn<'_, 'd>,
        _: &mut driver::Context<'_, 'd>,
    ) -> ops::ControlFlow<schedule::ready::Target<'d>> {
        ops::ControlFlow::Continue(())
    }

    fn receive<'input>(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        input: crate::Incoming<'input, 'w, 'd, Self::Wire, Self::Input>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> crate::Receiving<'w, 'd, Self::Wire, Self::Input>;

    fn finish(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        finish: ingress::Finish,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    );

    /// Schedules the bounded protocol-control finalizer. Marking is
    /// idempotent and does not close the transport.
    fn latch(self: pin::Pin<&mut Self>, key: pool::Key<'d, ID>);

    fn terminal(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> ingress::Terminal;

    fn eof(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        end: End,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    );

    fn close(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        cause: ingress::CloseCause,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    );

    fn flush(
        mut self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let received = Self::storage(self.as_mut())
            .recv()
            .flush(turn.reborrow().maint(), driver);
        if let Err(failure) = received {
            Self::close(
                self,
                failure.key,
                ingress::CloseCause::Receive(event::ParkError::Submit(failure.cause)),
                turn,
                driver,
            );
        }
    }

    fn dispatch(
        mut self: pin::Pin<&mut Self>,
        item: recv::Completion<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> ops::ControlFlow<recv::Completion<'d>>
    where
        <Self::Input as input::Mode<Self::Wire>>::Reserve:
            Choice<Self::Wire, <Self::Input as input::Mode<Self::Wire>>::Ingress>,
    {
        let token = item.token();
        match item.classify() {
            recv::Kind::Data(data) => {
                let targets = driver.driver_ref().targets::<ID>();
                if Self::admit(
                    self.as_mut(),
                    || targets.parse(token).ok().map(|op| route::Target::from(&op)),
                    turn.reborrow(),
                    driver,
                )
                .is_break()
                {
                    return ops::ControlFlow::Break(data.rejoin());
                }
                let work = match <crate::Ingress<Self::Wire, Self::Input> as Strategy<
                    Self::Wire,
                    Self::Input,
                >>::Admission::fresh(turn.reborrow(), driver)
                {
                    schedule::Capacity::Available(work) => work,
                    schedule::Capacity::Exhausted => {
                        return ops::ControlFlow::Break(data.rejoin());
                    }
                };
                <crate::Ingress<Self::Wire, Self::Input> as Strategy<Self::Wire, Self::Input>>::drive_data(self.as_mut(), data, work);
            }
            recv::Kind::Control(ctrl) => {
                ingress::drive_control(self.as_mut(), ctrl, turn.reborrow(), driver);
            }
        }
        Self::flush(self, turn, driver);
        ops::ControlFlow::Continue(())
    }

    fn resume(
        self: pin::Pin<&mut Self>,
        target: route::Target<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) where
        <Self::Input as input::Mode<Self::Wire>>::Reserve:
            Choice<Self::Wire, <Self::Input as input::Mode<Self::Wire>>::Ingress>,
    {
        <crate::Ingress<Self::Wire, Self::Input> as Strategy<Self::Wire, Self::Input>>::resume(
            self, target, turn, driver,
        );
    }
}
