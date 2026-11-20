use std::{ops, pin};

use dope_core::{
    driver::{self, route, route::Identity, schedule},
    io::event::net::recv,
};
use dope_net::{
    link::{
        egress::{data, reserve::demand},
        event,
        pool::{
            self,
            recv::{
                input, reserve::Choice,
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
    type Payload: data::raw::Payload<'d>;
    type Reserve: demand::Mode;
    type Input: Mode<Self::Wire> + Dispatch<Self::Wire>;

    type Context<'a>
    where
        Self: 'a;

    fn parts<'a>(
        self: pin::Pin<&'a mut Self>,
    ) -> (&'a mut crate::Inbound<'p, 'w, 'd, ID, Self>, Self::Context<'a>);

    fn storage<'a>(self: pin::Pin<&'a mut Self>) -> &'a mut crate::Inbound<'p, 'w, 'd, ID, Self> {
        Self::parts(self).0
    }

    fn receipt<'ctx>(
        context: &mut Self::Context<'ctx>,
        key: pool::Key<'d, ID>,
        state: &Self::State,
        token: &region::Token<region::Identity<'d>>,
    ) -> <Self::Input as input::Mode<Self::Wire>>::Receipt
    where
        Self: 'ctx;

    /// Admits input before its native completion or held tail is consumed.
    fn admit(
        self: pin::Pin<&mut Self>,
        _: impl FnOnce() -> Option<route::Target<'d, ID>>,
        _: schedule::Turn<'_, 'd>,
        _: &mut driver::Context<'_, 'd>,
    ) -> ops::ControlFlow<schedule::ready::Target<'d>> {
        ops::ControlFlow::Continue(())
    }

    fn receive<'ctx, 'a, 'input>(
        context: &mut Self::Context<'ctx>,
        entry: ingress::Entry<'a, 'p, 'w, 'd, ID, Self>,
        input: crate::Incoming<'input, 'w, 'd, Self::Wire, Self::Input>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> (
        Option<ingress::Entry<'a, 'p, 'w, 'd, ID, Self>>,
        crate::Receiving<Self::Wire, Self::Input>,
    )
    where
        Self: 'ctx;

    fn finish<'ctx, 'a>(
        context: &mut Self::Context<'ctx>,
        entry: ingress::Entry<'a, 'p, 'w, 'd, ID, Self>,
        finish: ingress::Finish,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Option<ingress::Entry<'a, 'p, 'w, 'd, ID, Self>>
    where
        Self: 'ctx;

    fn terminal<'ctx, 'a>(
        context: &mut Self::Context<'ctx>,
        entry: ingress::Entry<'a, 'p, 'w, 'd, ID, Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> (Option<ingress::Entry<'a, 'p, 'w, 'd, ID, Self>>, ingress::Terminal)
    where
        Self: 'ctx;

    fn eof<'ctx, 'a>(
        context: &mut Self::Context<'ctx>,
        entry: ingress::Entry<'a, 'p, 'w, 'd, ID, Self>,
        end: End,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Option<ingress::Entry<'a, 'p, 'w, 'd, ID, Self>>
    where
        Self: 'ctx;

    fn close<'ctx, 'a>(
        context: &mut Self::Context<'ctx>,
        entry: ingress::Entry<'a, 'p, 'w, 'd, ID, Self>,
        cause: ingress::CloseCause,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Option<ingress::Entry<'a, 'p, 'w, 'd, ID, Self>>
    where
        Self: 'ctx;

    fn flush(
        mut self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let received = Self::storage(self.as_mut())
            .recv()
            .flush(turn.reborrow().maint(), driver);
        if let Err(failure) = received {
            let (pool, mut context) = Self::parts(self);
            if let Ok(entry) = pool.entry(failure.key) {
                drop(Self::close(
                    &mut context,
                    entry,
                    ingress::CloseCause::Receive(event::ParkError::Submit(failure.cause)),
                    turn,
                    driver,
                ));
            }
        }
    }

    fn dispatch<K: route::Selection>(
        mut self: pin::Pin<&mut Self>,
        item: recv::Completion<'d, K>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> ops::ControlFlow<recv::Completion<'d, K>>
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
