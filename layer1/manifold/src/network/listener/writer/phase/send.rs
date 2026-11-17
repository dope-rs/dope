use std::pin;

use dope_core::{
    driver::{self, route::Identity, schedule},
    io::event::net::send::Completion,
};
use handler::{Runtime, send};

use crate::{
    app::env,
    network::listener::{
        self, conn, handler,
        writer::{self, phase},
    },
};

pub(super) fn complete<'n, 'p, 's, 'w, 'd: 'w, ID: Identity, A, T: dope_net::Transport>(
    source: &mut super::sender::Source<'_, 'n, 'p, 's, 'w, 'd, ID, A, T>,
    deadlines: &mut writer::deadline::Schedule<'d, ID>,
    writes: &'p writer::memory::Arena<'s, 'd, ID, A::Egress>,
    mut app: pin::Pin<&mut A>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
) where
    A: Runtime<'w, 'd, ID>,
{
    let (_, mut connection, _, _) = super::sender::view::<ID, A, T>(source, deadlines);
    let key = connection.key();
    connection.cancel_send();
    if let Some(body) = connection.state.send.retire().0 {
        writer::release_egress(app.as_mut(), key, body, handler::Disposition::Sent);
    }
    notify(source, deadlines, writes, app, turn, driver);
}

fn notify<'n, 'p, 's, 'w, 'd: 'w, ID: Identity, A, T: dope_net::Transport>(
    source: &mut super::sender::Source<'_, 'n, 'p, 's, 'w, 'd, ID, A, T>,
    deadlines: &mut writer::deadline::Schedule<'d, ID>,
    writes: &'p writer::memory::Arena<'s, 'd, ID, A::Egress>,
    app: pin::Pin<&mut A>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
) where
    A: Runtime<'w, 'd, ID>,
{
    let mut egress = writer::Egress::new(source.view(), writes, deadlines);
    send::settle(app, &mut egress, turn, driver);
}

pub(in crate::network::listener) fn pump_send<'w, 'd: 'w, ID: Identity, A, E>(
    mut listener: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
    done: Completion<'d>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
) where
    A: handler::Application<'d, ID> + Runtime<'w, 'd, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    use super::sender;
    use dope_net::link::{event::{DirectStatus, SendFailure, SendOutcome}, pool::send::Completion};
    use o3::depot::raw::Proof;
    use writer::deadline::Slot;

    let this = listener.as_mut().project();
    let writer::Owner { pool, deadlines, writes } = this.owner;
    pool.settle(
        done, driver, &mut (this.app, this.accept, deadlines),
        |_, _| {},
        |(app, _, _), key, body, _| {
            writer::release_payload(app.as_mut(), key, body, handler::Disposition::Sent);
        },
        |ctx, completion, driver| {
            let source = match completion {
                Completion::Direct(direct) => {
                    let (mut owner, status) = direct.into_parts();
                    let key = owner.connection().key();
                    match status {
                        DirectStatus::Sent(done) => {
                            let send = &mut owner.connection().state.send;
                            if let Some(state) = send.direct() {
                                state.progress.settle(done);
                                if state.progress.error().is_none() {
                                    if state.progress.has_more() {
                                        let mut source = owner.publish(|conn| {
                                            // SAFETY: publication retains this writer's admitted slot and deadline lane.
                                            Slot::from(unsafe { Proof::new(((conn, &mut *ctx.2), key)) }).cancel_send();
                                        });
                                        advance::<ID, A, _>(&mut source, ctx.2, *writes, ctx.0.as_mut(), turn.reborrow(), driver);
                                        let mut entry = source.into_entry();
                                        if phase::finish(&mut entry, ctx.0.as_ref()) {
                                            drop(writer::close(Ok(entry), *writes, ctx, turn, driver));
                                        }
                                        return;
                                    }
                                    let direct = send.take(None);
                                    let publish = || owner.publish(|conn| {
                                        // SAFETY: publication retains this writer's admitted slot and deadline lane.
                                        Slot::from(unsafe { Proof::new(((conn, &mut *ctx.2), key)) }).cancel_send();
                                    });
                                    let mut source = match direct {
                                        Some(direct) => direct.retire(publish, |_, body| {
                                            writer::release_egress(ctx.0.as_mut(), key, body, handler::Disposition::Sent);
                                        }),
                                        None => publish(),
                                    };
                                    notify(&mut source, ctx.2, *writes, ctx.0.as_mut(), turn.reborrow(), driver);
                                    let mut entry = source.into_entry();
                                    if phase::finish(&mut entry, ctx.0.as_ref()) {
                                        drop(writer::close(Ok(entry), *writes, ctx, turn, driver));
                                    }
                                    return;
                                }
                            }
                            owner.publish(|conn| {
                                // SAFETY: publication retains this writer's admitted slot and deadline lane.
                                Slot::from(unsafe { Proof::new(((conn, &mut *ctx.2), key)) }).cancel_send();
                            })
                        }
                        status => {
                            let failure = match status {
                                DirectStatus::Rejected(error) => Some(conn::Transmit::Rejected(SendFailure::Direct(error))),
                                DirectStatus::Failed(error) => Some(conn::Transmit::Io(SendFailure::Direct(error))),
                                DirectStatus::Closed => None,
                                DirectStatus::Sent(_) => unreachable!("direct"),
                            };
                            let direct = owner.connection().state.send.take(failure);
                            let publish = || owner.publish(|conn| {
                                // SAFETY: publication retains this writer's admitted slot and deadline lane.
                                Slot::from(unsafe { Proof::new(((conn, &mut *ctx.2), key)) }).cancel_send();
                            });
                            match direct {
                                Some(direct) => direct.retire(publish, |_, body| {
                                    writer::release_egress(ctx.0.as_mut(), key, body, handler::Disposition::Closed);
                                }),
                                None => publish(),
                            }
                        }
                    }
                }
                Completion::Ready(owner, outcome) => {
                    let mut source = owner.publish(|conn| {
                        if !matches!(outcome, SendOutcome::Pending) {
                            let key = conn.key();
                            // SAFETY: publication retains this writer's admitted slot and deadline lane.
                            Slot::from(unsafe { Proof::new(((conn, &mut *ctx.2), key)) }).cancel_send();
                        }
                    });
                    let failure = match outcome {
                        SendOutcome::Queued { .. } | SendOutcome::WriteShutdown => {
                            notify(&mut source, ctx.2, *writes, ctx.0.as_mut(), turn.reborrow(), driver);
                            advance::<ID, A, _>(&mut source, ctx.2, *writes, ctx.0.as_mut(), turn.reborrow(), driver);
                            let mut entry = source.into_entry();
                            if phase::finish(&mut entry, ctx.0.as_ref()) {
                                drop(writer::close(Ok(entry), *writes, ctx, turn, driver));
                            }
                            return;
                        }
                        SendOutcome::Terminal => {
                            sender::commit_end::<ID, A, _>(&mut source, ctx.2, ctx.0.as_mut(), driver);
                            let mut entry = source.into_entry();
                            if phase::finish(&mut entry, ctx.0.as_ref()) {
                                drop(writer::close(Ok(entry), *writes, ctx, turn, driver));
                            }
                            return;
                        }
                        SendOutcome::Rejected { failure } => Some(conn::Transmit::Rejected(failure)),
                        SendOutcome::Failed { failure } => Some(conn::Transmit::Io(failure)),
                        SendOutcome::Invalid { error } => Some(conn::Transmit::Invalid(error)),
                        SendOutcome::Close => None,
                        SendOutcome::Pending => return,
                    };
                    if let Some(failure) = failure {
                        let key = source.view().connection.key();
                        if let Some(body) = source.view().connection.state.send.fail(failure) {
                            writer::release_egress(ctx.0.as_mut(), key, body, handler::Disposition::Closed);
                        }
                    }
                    source
                }
            };
            drop(writer::close(Ok(source.into_entry()), *writes, ctx, turn, driver));
        },
    );
}

fn advance<'n, 'p, 's, 'w, 'd: 'w, ID: Identity, A, T: dope_net::Transport>(
    source: &mut super::sender::Source<'_, 'n, 'p, 's, 'w, 'd, ID, A, T>,
    deadlines: &mut writer::deadline::Schedule<'d, ID>,
    writes: &'p writer::memory::Arena<'s, 'd, ID, A::Egress>,
    mut app: pin::Pin<&mut A>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
) where A: Runtime<'w, 'd, ID>,
{
    if matches!(super::sender::commit_chunk::<ID, A, _>(
        source, deadlines, app.as_mut(), phase::Wake::Send, driver,
    ), writer::flow::Flow::Done) {
        complete(source, deadlines, writes, app, turn, driver);
    }
}
