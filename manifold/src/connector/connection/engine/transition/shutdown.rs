use std::pin;

use close::ClosePhase as _;
use dope_core::driver::{retained, schedule};
use dope_net::link::pool::pending;

use crate::connector::{
    app, attempt, auxiliary,
    auxiliary::Ownership as _,
    connection::{
        self,
        engine::{scheduling::phase, transition::close},
    },
    lifecycle,
};

pub(in crate::connector) trait ShutdownPhase<'d, const ID: u8, A, S, E>
where
    A: app::Lifecycle<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
{
    fn drain_shutdown(
        self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );

    fn flush_cancellations(self: pin::Pin<&mut Self>, turn: schedule::Turn<'_, 'd>);
}

impl<'d, const ID: u8, A, S, E, X> ShutdownPhase<'d, ID, A, S, E>
    for connection::Engine<'d, ID, A, S, E, X>
where
    A: app::Lifecycle<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: auxiliary::Mode<'d, A::Send, ID>,
{
    fn drain_shutdown(
        mut self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        if !matches!(
            self.as_ref().project_ref().schedule.shutdown,
            phase::Shutdown::Closing(_)
        ) {
            return;
        }
        loop {
            if !turn.reborrow().maintenance().take() {
                return;
            }
            let next = {
                let this = self.as_mut().project();
                let index = match &this.schedule.shutdown {
                    phase::Shutdown::Open | phase::Shutdown::Done => return,
                    phase::Shutdown::Closing(index) => *index,
                };
                let next = this.pool.inspection().capacity().slot(index);
                this.schedule.shutdown = match next {
                    Some(_) => phase::Shutdown::Closing(index + 1),
                    None => phase::Shutdown::Done,
                };
                next
            };
            let Some(index) = next else {
                break;
            };
            let Some(key) = self.as_ref().project_ref().pool.key_at(index) else {
                continue;
            };
            self.as_mut()
                .close_slot(key, lifecycle::CloseReason::Local, turn.reborrow(), driver);
        }
    }

    fn flush_cancellations(mut self: pin::Pin<&mut Self>, turn: schedule::Turn<'_, 'd>) {
        loop {
            let work = turn.reborrow().maintenance();
            if work.remaining() == 0 {
                return;
            }
            let cancel = {
                let this = self.as_mut().project();
                attempt::Control::take_cancel(this.controller)
            };
            let Some((attempt, key)) = cancel else {
                return;
            };
            work.take();
            let this = self.as_mut().project();
            let Some((slot, handle)) = pending::Pending::of(this.pool).get(key) else {
                continue;
            };
            if slot.state.owner.attempt() == Some(attempt) {
                handle.mark(pending::Action::Close);
            }
        }
    }
}
