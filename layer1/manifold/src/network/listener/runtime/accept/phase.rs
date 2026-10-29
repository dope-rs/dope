use std::pin;

use dope_core::{
    driver::{retained, route::Identity, schedule},
    io::{
        event::net::{accept, tuning},
        socket::pending,
    },
};
use dope_net::link::pool::{
    self,
    change::accept::{Completion, Outcome},
};
use o3::{base::Call as _, depot::capacity};

use crate::{
    app::{env, receive},
    network::listener::{
        self, conn,
        handler::{self, failures},
        runtime::{
            accept::{Incoming, Rejection, peers},
            control,
        },
        writer::phase,
    },
};

pub(in crate::network::listener) fn accept<'w, 'd: 'w, ID: Identity, A, E>(
    mut root: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
    done: accept::Completion<'d>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
) where
    A: handler::Application<'d, ID>,
    A::WireFailure: failures::Policy<'w, 'd, A, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    let result = {
        let mut this = root.as_mut().project();
        let (fd, peer) = match this.accept.as_mut().complete(done, driver) {
            Incoming::Accepted(fd, ip) => (fd, ip),
            Incoming::Invalid(fd, cause) => {
                if let Some(peer) = <A::WireFailure as failures::Policy<'w, 'd, A, ID>>::peer() {
                    peer.call((this.app.as_mut(), cause));
                }
                fd.close(driver.driver());
                return;
            }
            Incoming::Failed {
                target,
                cause,
                flow,
                defer,
            } => {
                let retry = if cause.is_retryable() || defer {
                    true
                } else if cause.is_resource() {
                    this.accept
                        .as_mut()
                        .defer(turn.clock().now(), driver.driver());
                    true
                } else {
                    false
                };
                if !retry {
                    this.accept
                        .as_mut()
                        .failed(listener::AcceptError::Completion(cause));
                }
                driver.accept_error(target, cause, flow);
                if !retry {
                    this.accept.as_mut().stop_accept(driver.driver());
                }
                return;
            }
            Incoming::Rejected => return,
        };
        if !this.schedule.is_open() {
            fd.close(driver.driver());
            return;
        }
        let mut accept = this.accept.as_mut();
        let app = this.app.as_mut();
        let seed = driver.driver_ref().random_seed();
        let result = this.owner.pool_mut().accept_with(
            accept.as_ref().get_ref().capacity,
            fd,
            accept.as_ref().options(),
            |key| {
                let admit = match peer {
                    Some(ip) => match accept.as_mut().admit(key, ip) {
                        Ok(admit) => admit,
                        Err(error) => return Err(Rejection::Peer(error)),
                    },
                    None => None,
                };
                let conn = match app.as_ref().connection(seed) {
                    Ok(conn) => conn,
                    Err(error) => {
                        if let Some(admit) = admit {
                            accept.as_mut().release(admit);
                        }
                        return Err(Rejection::Application(error));
                    }
                };
                Ok(conn::State::<ID, A::Conn, A::Egress>::new(conn, admit))
            },
            driver,
        );
        if this.owner.pool().inspect().available() == capacity::Count::EMPTY {
            accept.pause(driver.driver());
        }
        result
    };
    match result {
        Ok(Outcome::Ready(key)) => {
            finish(root.as_mut(), key, turn.reborrow(), driver);
        }
        Ok(Outcome::Failed(key, cause)) => {
            if let Some(slot) = root.as_mut().project().owner.pool_mut().get_mut(key) {
                match cause {
                    pending::Cause::Submit(error) => {
                        slot.state.fail(conn::Failure::Submit(error));
                        slot.sending().discard();
                    }
                    pending::Cause::Option(error) => {
                        slot.state.fail(conn::Failure::Option(error));
                    }
                }
            }
            control::close_slot(root.as_mut(), key, turn.reborrow(), driver);
        }
        Ok(Outcome::IdentityExhausted) => {
            root.as_mut()
                .project()
                .accept
                .failed(listener::AcceptError::IdentityExhausted);
            root.as_mut().seal(driver.driver());
        }
        Ok(Outcome::Rejected(Rejection::Application(cause))) => {
            A::build_failed(root.as_mut().project().app, cause);
        }
        Ok(Outcome::Rejected(Rejection::Peer(peers::Full)))
        | Ok(Outcome::Pending | Outcome::Unavailable) => {}
        Err(error) => <A::WireFailure as failures::Policy<'w, 'd, A, ID>>::open_failed(
            root.as_mut().project().app.as_mut(),
            error,
        ),
    }
}

pub(in crate::network::listener) fn tune<'w, 'd: 'w, ID: Identity, A, E>(
    mut root: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
    done: tuning::Completion<'d>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
) -> Result<(), tuning::Completion<'d>>
where
    A: handler::Application<'d, ID>,
    A::WireFailure: failures::Policy<'w, 'd, A, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    let result = root.as_mut().project().owner.pool_mut().tuned(done);
    let key = match result {
        Completion::Ready(key) => key,
        Completion::Failed(key, error) => {
            if let Some(slot) = root.as_mut().project().owner.pool_mut().get_mut(key) {
                slot.state.fail(conn::Failure::Tuning(error));
            }
            control::close_slot(root.as_mut(), key, turn.reborrow(), driver);
            return Ok(());
        }
        Completion::Cancelled(key) => {
            control::close_slot(root.as_mut(), key, turn.reborrow(), driver);
            return Ok(());
        }
        Completion::Stale(done) => return Err(done),
    };
    finish(root.as_mut(), key, turn, driver);
    Ok(())
}

pub(in crate::network::listener) fn finish<'w, 'd: 'w, ID: Identity, A, E>(
    mut root: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
    key: pool::Key<'d, ID>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
) where
    A: handler::Application<'d, ID>,
    A::WireFailure: failures::Policy<'w, 'd, A, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    let outcome = {
        let mut this = root.as_mut().project();
        this.owner.pool().inspect().refresh_wake(key);
        let now = driver.driver_ref().clock().now();
        match this.owner.egress_mut(key) {
            Some(mut egress) => {
                egress.connection.open(now);
                let outcome = A::accept(this.app.as_mut(), egress.context(turn.reborrow().app()));
                if matches!(outcome, receive::Outcome::CloseAfter) {
                    egress.connection.close_after();
                }
                outcome
            }
            None => return,
        }
    };
    match outcome {
        receive::Outcome::Failed(_) => control::close_slot(root.as_mut(), key, turn, driver),
        receive::Outcome::Continue | receive::Outcome::CloseAfter => {
            let received = root
                .as_mut()
                .project()
                .owner
                .pool_mut()
                .recv()
                .arm(key, driver);
            if let Err(failure) = received {
                control::receive_failed(root, failure, turn, driver);
                return;
            }
            phase::close_ready(root.as_mut(), key, turn, driver);
        }
    }
}
