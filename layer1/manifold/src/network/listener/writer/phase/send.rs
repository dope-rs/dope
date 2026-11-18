use std::{mem::MaybeUninit, pin};

use dope_core::{
    driver::{self, route::Identity, schedule},
    io::event::net::send::Completion,
};
use handler::{Runtime, send};
use o3::mem::slot;

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
    connection.state.send.release(None, |body| {
        writer::release_egress(app.as_mut(), key, body, handler::Disposition::Sent);
    });
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
    use dope_net::link::event::SendOutcome;
    use o3::depot::raw::Proof;
    use writer::deadline::Slot;

    let this = listener.as_mut().project();
    let writer::Owner { pool, deadlines, writes } = this.owner;
    let mut ctx = (this.app, this.accept, deadlines);
    let mut body = MaybeUninit::uninit();
    let place = slot::Vacant::new(&mut body);
    let _ = pool.settle(
        done, driver, &mut ctx,
        |_, _| {},
        |(app, _, _), key, body, _| {
            writer::release_payload(app.as_mut(), key, body, handler::Disposition::Sent);
        },
        |ctx, connection, outcome| {
            let key = connection.key();
            let retired = match outcome {
                SendOutcome::Pending => return (Action::Return, None),
                SendOutcome::Direct { consumed } => {
                    let send = &mut connection.state.send;
                    match send.progress() {
                        Some(progress) => {
                            progress.settle(consumed);
                            if progress.error().is_some() {
                                (Action::Close, send.take(None, place))
                            } else if progress.has_more() {
                                (Action::Advance, None)
                            } else {
                                (Action::Notify, send.take(None, place))
                            }
                        }
                        None => (Action::Close, None),
                    }
                }
                SendOutcome::DirectClosed => (Action::Close, connection.state.send.take(None, place)),
                SendOutcome::Queued { .. } | SendOutcome::WriteShutdown => (Action::Queued, None),
                SendOutcome::Terminal => (Action::Terminal, None),
                SendOutcome::Rejected { failure } => (
                    Action::Close,
                    connection.state.send.take(Some(conn::Transmit::Rejected(failure)), place),
                ),
                SendOutcome::Failed { failure } => (
                    Action::Close,
                    connection.state.send.take(Some(conn::Transmit::Io(failure)), place),
                ),
                SendOutcome::Invalid { error } => (
                    Action::Close,
                    connection.state.send.take(Some(conn::Transmit::Invalid(error)), place),
                ),
                SendOutcome::Close => (Action::Close, None),
            };
            // SAFETY: settlement retains this connection and its original deadline lane.
            Slot::from(unsafe { Proof::new(((connection, &mut *ctx.2), key)) }).cancel_send();
            retired
        },
        |ctx, (action, body), mut source, driver| {
            if let Some(body) = body {
                let cause = match action {
                    Action::Close => handler::Disposition::Closed,
                    _ => handler::Disposition::Sent,
                };
                let key = source.view().connection.key();
                writer::release_egress(ctx.0.as_mut(), key, body.into_inner(), cause);
            }
            match action {
                Action::Return => return,
                Action::Advance => {
                    advance::<ID, A, _>(&mut source, ctx.2, *writes, ctx.0.as_mut(), turn.reborrow(), driver);
                }
                Action::Notify => {
                    notify(&mut source, ctx.2, *writes, ctx.0.as_mut(), turn.reborrow(), driver);
                }
                Action::Queued => {
                    notify(&mut source, ctx.2, *writes, ctx.0.as_mut(), turn.reborrow(), driver);
                    advance::<ID, A, _>(&mut source, ctx.2, *writes, ctx.0.as_mut(), turn.reborrow(), driver);
                }
                Action::Terminal => {
                    sender::commit_end::<ID, A, _>(&mut source, ctx.2, ctx.0.as_mut(), driver);
                }
                Action::Close => {
                    drop(writer::close(Ok(source.into_entry()), *writes, ctx, turn, driver));
                    return;
                }
            }
            let mut entry = source.into_entry();
            if phase::finish(&mut entry, ctx.0.as_ref()) {
                drop(writer::close(Ok(entry), *writes, ctx, turn, driver));
            }
        },
    );
}

enum Action {
    Return,
    Advance,
    Notify,
    Queued,
    Terminal,
    Close,
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
