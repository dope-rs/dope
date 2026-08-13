use std::pin;

use dope_core::{
    driver::{self, retained, schedule},
    io::socket,
};
use dope_net::link::pool::{self, pending, transition::open};

use crate::{
    connector::{
        app, attempt,
        auxiliary::{self, Ownership as _},
        connection::{
            self,
            engine::{scheduling::deadline, transition::close},
        },
        lifecycle,
    },
    timing,
};

fn lane_index(primary_capacity: u32, target_index: usize) -> Option<usize> {
    (primary_capacity as usize).checked_add(target_index)
}

pub(in crate::connector) trait AuxiliaryPhase<'d, const ID: u8, A, S, E, X>
where
    A: app::Lifecycle<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: auxiliary::Mode<'d, A::Send, ID>,
{
    fn poll_requests(
        self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );

    fn cancel_abandoned(
        self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );
}

impl<'d, const ID: u8, A, S, E, X> AuxiliaryPhase<'d, ID, A, S, E, X>
    for connection::Engine<'d, ID, A, S, E, X>
where
    A: app::Lifecycle<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: auxiliary::Mode<'d, A::Send, ID>,
{
    fn poll_requests(
        mut self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        while self.as_ref().project_ref().auxiliary.has_requests() {
            let Some(permit) = turn.reborrow().application().permit() else {
                return;
            };
            let request = {
                let this = self.as_mut().project();
                this.auxiliary.take_request(permit, driver.region_token())
            };
            let Some(request) = request else {
                return;
            };
            self.as_mut()
                .submit_auxiliary(request, turn.reborrow(), driver);
        }
    }

    fn cancel_abandoned(
        mut self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        while self.as_ref().project_ref().auxiliary.has_cancellations() {
            let Some(permit) = schedule::MaintenancePermit::try_take(turn.reborrow().maintenance())
            else {
                return;
            };
            let target = {
                let this = self.as_mut().project();
                this.auxiliary
                    .take_cancellation(permit, driver.region_token())
            };
            let Some(target) = target else {
                return;
            };
            let key = {
                let this = self.as_ref().project_ref();
                let Some(index) = lane_index(*this.primary_capacity, target.index()) else {
                    continue;
                };
                let Some(lane) = this.pool.inspection().capacity().slot(index) else {
                    continue;
                };
                let Some(key) = this.pool.key_at(lane) else {
                    continue;
                };
                key
            };
            let ticket = {
                let this = self.as_mut().project();
                let Some((slot, handle)) = pending::Mut::of(this.pool).get(key) else {
                    continue;
                };
                if slot.state.owner.auxiliary_target() != Some(target) {
                    continue;
                }
                let Some(ticket) = slot.state.owner.take_ticket() else {
                    continue;
                };
                slot.abort();
                slot.state.closing.request_permanent();
                slot.state.request_close(lifecycle::CloseReason::Local);
                handle.mark(pending::Action::Close);
                ticket
            };
            self.as_mut().project().auxiliary.complete(
                ticket,
                Err(auxiliary::Error::Transport),
                driver.region_token(),
            );
        }
    }
}

trait SubmitAuxiliary<'d, const ID: u8, A, S, E, X>
where
    A: app::Lifecycle<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: auxiliary::Mode<'d, A::Send, ID>,
{
    fn submit_auxiliary(
        self: pin::Pin<&mut Self>,
        request: (X::RequestAuthority, A::Send),
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );
}

impl<'d, const ID: u8, A, S, E, X> SubmitAuxiliary<'d, ID, A, S, E, X>
    for connection::Engine<'d, ID, A, S, E, X>
where
    A: app::Lifecycle<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: auxiliary::Mode<'d, A::Send, ID>,
{
    fn submit_auxiliary(
        mut self: pin::Pin<&mut Self>,
        request: (X::RequestAuthority, A::Send),
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let target = X::request_target(&request.0);
        let peer_and_options = {
            let this = self.as_ref().project_ref();
            this.pool.get(target.key()).and_then(|slot| {
                slot.state
                    .owner
                    .attempt()
                    .zip(slot.state.peer)
                    .map(|(_, peer)| (peer, slot.state.options))
            })
        };
        let Some((peer, options)) = peer_and_options else {
            return reject(self.as_mut(), request, auxiliary::Error::Stale, driver);
        };
        let Ok(socket) = socket::StreamSpec::for_peer(&peer) else {
            return reject(self.as_mut(), request, auxiliary::Error::NoTarget, driver);
        };
        let lane = {
            let this = self.as_ref().project_ref();
            let Some(index) = lane_index(*this.primary_capacity, target.index()) else {
                return reject(self.as_mut(), request, auxiliary::Error::Capacity, driver);
            };
            let Some(lane) = this.pool.inspection().capacity().slot(index) else {
                return reject(self.as_mut(), request, auxiliary::Error::Capacity, driver);
            };
            lane
        };
        let submitted = {
            let this = self.as_mut().project();
            let app = &*this.app;
            this.pool.submit_socket_at(
                lane,
                socket,
                (request, peer, options),
                |(request, peer, options)| {
                    let (authority, payload) = request;
                    let owner = X::auxiliary(authority);
                    (
                        connection::State::new(app::Application::connection(app), owner, options),
                        Some(peer),
                        payload,
                    )
                },
                driver,
            )
        };
        match submitted {
            Ok(open::Outcome::Submitted {
                key,
                output: payload,
            }) => {
                let this = self.as_mut().project();
                if this
                    .pool
                    .try_stage(driver.region_token(), key, payload)
                    .is_ok()
                {
                    let now = driver.turn_now();
                    if deadline::DeadlinePhase::arm_timeout(
                        self.as_mut(),
                        key,
                        lifecycle::TimeoutKind::Auxiliary,
                        now,
                        <E::Timing as timing::Policy>::SEND_DEADLINE,
                    ) {
                        return;
                    }
                    close::ClosePhase::abort_slot(
                        self,
                        key,
                        lifecycle::CloseReason::Timeout(lifecycle::TimeoutKind::Auxiliary),
                        turn,
                        driver,
                    );
                    return;
                }
                let ticket = retire_ticket(self.as_mut(), key);
                if let Some(ticket) = ticket {
                    self.as_mut().project().auxiliary.complete(
                        ticket,
                        Err(auxiliary::Error::Capacity),
                        driver.region_token(),
                    );
                }
            }
            Ok(open::Outcome::Deferred { cause, input }) => {
                let error = match cause {
                    open::Deferred::Capacity
                    | open::Deferred::SubmissionBackpressure
                    | open::Deferred::WireBackpressure => auxiliary::Error::Capacity,
                };
                reject(self, input.0, error, driver);
            }
            Err(rejected) => {
                let ((request, _, _), _) = rejected.into_parts();
                reject(self, request, auxiliary::Error::Wire, driver);
            }
        }
    }
}

fn retire_ticket<'d, const ID: u8, A, S, E, X>(
    engine: pin::Pin<&mut connection::Engine<'d, ID, A, S, E, X>>,
    key: pool::Key<'d, ID>,
) -> Option<auxiliary::Ticket<'d, ID>>
where
    A: app::Lifecycle<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: auxiliary::Mode<'d, A::Send, ID>,
{
    let this = engine.project();
    let (slot, handle) = pending::Mut::of(this.pool).get(key)?;
    let ticket = slot.state.owner.take_ticket()?;
    slot.state.closing.request_permanent();
    handle.mark(pending::Action::Close);
    Some(ticket)
}

fn reject<'d, const ID: u8, A, S, E, X>(
    engine: pin::Pin<&mut connection::Engine<'d, ID, A, S, E, X>>,
    request: (X::RequestAuthority, A::Send),
    error: auxiliary::Error,
    driver: &mut driver::Context<'_, 'd>,
) where
    A: app::Lifecycle<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: auxiliary::Mode<'d, A::Send, ID>,
{
    let (authority, _) = request;
    engine.project().auxiliary.complete(
        X::into_ticket(authority),
        Err(error),
        driver.region_token(),
    );
}
