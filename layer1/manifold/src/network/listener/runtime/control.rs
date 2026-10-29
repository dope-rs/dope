use std::pin;

use conn::deadline::Arm;
use dope_core::driver::{retained, route::Identity, schedule, schedule::clock};
use dope_net::link::pool::{self, pending, recv};
use o3::mem::quota;

use crate::{
    app::env,
    network::listener::{self, conn, handler, writer::phase},
};

pub(in crate::network::listener) fn admit<'w, 'd: 'w, ID: Identity, A, E>(
    mut root: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
) where
    A: handler::Application<'d, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    if !root.as_ref().project_ref().schedule.is_sealing() {
        return;
    }
    loop {
        let grant = turn.reborrow().maint().claim(|| {
            let this = root.as_mut().project();
            let cap = this.owner.pool().inspect().capacity();
            this.schedule.take_seal(cap)
        });
        let (index, permit) = match grant {
            quota::Admission::Item(pair) => pair,
            quota::Admission::Empty => break,
            quota::Admission::Exhausted => return,
        };
        permit.run(|| {
            let this = root.as_mut().project();
            let Some(key) = this.owner.pool().inspect().key_at(index) else {
                return;
            };
            if let Some(slot) = this.owner.pool_mut().get_mut(key) {
                slot.close_after();
            }
            phase::close_ready(root.as_mut(), key, turn.reborrow(), driver);
        });
    }
}

pub(in crate::network::listener) fn shutdown<'w, 'd: 'w, ID: Identity, A, E>(
    mut root: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
) where
    A: handler::Application<'d, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    if !root.as_ref().project_ref().schedule.is_closing() {
        return;
    }
    root.as_mut().project().accept.stop_accept(driver);
    loop {
        let grant = turn.reborrow().maint().claim(|| {
            let this = root.as_mut().project();
            let cap = this.owner.pool().inspect().capacity();
            this.schedule.take_close(cap)
        });
        let (index, permit) = match grant {
            quota::Admission::Item(pair) => pair,
            quota::Admission::Empty => break,
            quota::Admission::Exhausted => return,
        };
        permit.run(|| {
            let Some(key) = root
                .as_ref()
                .project_ref()
                .owner
                .pool()
                .inspect()
                .key_at(index)
            else {
                return;
            };
            close_slot(root.as_mut(), key, turn.reborrow(), driver);
        });
    }
}

pub(in crate::network::listener) fn discard_slot<'w, 'd: 'w, ID: Identity, A, E>(
    mut root: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
    key: pool::Key<'d, ID>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
) where
    A: handler::Application<'d, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    if let Some(slot) = root.as_mut().project().owner.pool_mut().get_mut(key) {
        slot.discard();
    }
    close_slot(root, key, turn, driver);
}

pub(in crate::network::listener) fn receive_failed<'w, 'd: 'w, ID: Identity, A, E>(
    mut root: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
    failure: recv::Failure<'d, ID>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
) where
    A: handler::Application<'d, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    if let Some(slot) = root
        .as_mut()
        .project()
        .owner
        .pool_mut()
        .get_mut(failure.key)
    {
        slot.state.fail(conn::Failure::Submit(failure.cause));
    }
    close_slot(root, failure.key, turn, driver);
}

pub(in crate::network::listener) fn expire<'w, 'd: 'w, ID: Identity, A, E>(
    mut root: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
    now: clock::Moment<'d>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
) where
    A: handler::Application<'d, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    loop {
        let grant = turn.reborrow().maint().claim(|| {
            let this = root.as_mut().project();
            this.owner.pop(now)
        });
        let ((key, kind), permit) = match grant {
            quota::Admission::Item(pair) => pair,
            quota::Admission::Empty | quota::Admission::Exhausted => break,
        };
        use crate::network::listener::writer::deadline::Kind;

        permit.run(|| match kind {
            Kind::Inbound | Kind::Send | Kind::Drain => {
                discard_slot(root.as_mut(), key, turn.reborrow(), driver);
            }
            Kind::Absolute => {
                let drain = {
                    let this = root.as_mut().project();
                    this.owner.drain()
                };
                let Some(drain) = drain else {
                    discard_slot(root.as_mut(), key, turn.reborrow(), driver);
                    return;
                };
                let drain = {
                    let mut this = root.as_mut().project();
                    match this.owner.egress_mut(key) {
                        Some(mut egress) => {
                            egress.connection.ensure_drain(now, drain);
                            Some(A::drain(
                                this.app.as_mut(),
                                egress.context(turn.reborrow().app()),
                                driver,
                            ))
                        }
                        None => None,
                    }
                };
                match drain {
                    Some(handler::Drain::Pending) => {}
                    Some(handler::Drain::Complete) => {
                        if let Some(slot) = root.as_mut().project().owner.pool_mut().get_mut(key) {
                            slot.close_after();
                        }
                        phase::close_ready(root.as_mut(), key, turn.reborrow(), driver);
                    }
                    Some(handler::Drain::Discard) => {
                        discard_slot(root.as_mut(), key, turn.reborrow(), driver);
                    }
                    None => {}
                }
            }
            Kind::Application => {
                enum Timeout {
                    Continue,
                    CloseAfter,
                    Discard,
                }
                let action = {
                    let mut this = root.as_mut().project();
                    match this.owner.egress_mut(key) {
                        Some(mut egress) => match A::timeout(
                            this.app.as_mut(),
                            egress.context(turn.reborrow().app()),
                            driver,
                        ) {
                            handler::Timeout::Discard => Timeout::Discard,
                            handler::Timeout::CloseAfter => {
                                egress.connection.end_discard();
                                egress.connection.close_after();
                                Timeout::CloseAfter
                            }
                            handler::Timeout::Continue(update) => {
                                let arm = egress.connection.apply(update);
                                match arm {
                                    Arm::Armed => Timeout::Continue,
                                    Arm::Disabled | Arm::Sealed | Arm::Overflow(_) => {
                                        Timeout::Discard
                                    }
                                }
                            }
                        },
                        None => Timeout::Continue,
                    }
                };
                match action {
                    Timeout::Discard => discard_slot(root.as_mut(), key, turn.reborrow(), driver),
                    Timeout::CloseAfter => {
                        phase::close_ready(root.as_mut(), key, turn.reborrow(), driver)
                    }
                    Timeout::Continue => {}
                }
            }
        });
    }
}

/// Preserves explicit closure while terminal output converges.
pub(in crate::network::listener) fn close_slot<'w, 'd: 'w, ID: Identity, A, E>(
    root: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
    key: pool::Key<'d, ID>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
) where
    A: handler::Application<'d, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    let term = {
        let this = root.as_ref().project_ref();
        this.owner.pool().inspect().get(key).map(|slot| {
            slot.io().close();
            (slot.terminal.is_pending(), slot.terminal.has_send())
        })
    };
    if let Some((true, send)) = term {
        if send
            && let Some(handle) =
                pending::Pending::of(root.as_ref().project_ref().owner.pool()).at(key)
        {
            handle.wake();
        }
        return;
    }
    let this = root.project();
    this.owner.close(this.app, this.accept, key, turn, driver);
}
