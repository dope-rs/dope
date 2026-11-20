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
    use dope_net::link::{event::SendOutcome, slot::types};
    use o3::depot::raw::Proof;
    use writer::deadline::Slot;

    let this = listener.as_mut().project();
    let writer::Owner { pool, deadlines, writes } = this.owner;
    let mut ctx = (this.app, this.accept, deadlines);
    let _ = pool.settle(
        done, driver, &mut ctx,
        |_, _| {},
        |(app, _, _), key, body, _| {
            writer::release_payload(app.as_mut(), key, body, handler::Disposition::Sent);
        },
        |ctx, outcome, mut settlement, space, driver| {
            let cancel = |connection: &mut types::Connection<'w, 'd, ID, A::Wire, _>| {
                let key = connection.key();
                // SAFETY: settlement retains this connection and its original deadline lane.
                Slot::from(unsafe { Proof::new(((connection, &mut *ctx.2), key)) }).cancel_send();
            };
            let mut body = MaybeUninit::uninit();
            let place = slot::Vacant::new(&mut body);
            let (source, settled);
            let close = 'complete: {
                let (disposition, body) = 'retire: {
                    let failure = match outcome {
                        SendOutcome::Pending => return (settlement.finish(space, |_| {}).1, ()),
                        SendOutcome::Direct { consumed } => {
                            if let Some(ended) = settlement.connection().state.send.settle(consumed, place) {
                                break 'retire ended;
                            }
                            (source, settled) = settlement.finish(space, cancel);
                            advance::<ID, A, _>(source, ctx.2, *writes, ctx.0.as_mut(), turn.reborrow(), driver);
                            break 'complete false;
                        }
                        SendOutcome::Queued { .. } | SendOutcome::WriteShutdown => {
                            (source, settled) = settlement.finish(space, cancel);
                            notify(source, ctx.2, *writes, ctx.0.as_mut(), turn.reborrow(), driver);
                            advance::<ID, A, _>(source, ctx.2, *writes, ctx.0.as_mut(), turn.reborrow(), driver);
                            break 'complete false;
                        }
                        SendOutcome::Terminal => {
                            (source, settled) = settlement.finish(space, cancel);
                            sender::commit_end::<ID, A, _>(source, ctx.2, ctx.0.as_mut(), driver);
                            break 'complete false;
                        }
                        SendOutcome::DirectClosed => None,
                        SendOutcome::Rejected { failure } => Some(conn::Transmit::Rejected(failure)),
                        SendOutcome::Failed { failure } => Some(conn::Transmit::Io(failure)),
                        SendOutcome::Invalid { error } => Some(conn::Transmit::Invalid(error)),
                        SendOutcome::Close => break 'retire (handler::Disposition::Closed, None),
                    };
                    (handler::Disposition::Closed, settlement.connection().state.send.take(failure, place))
                };
                (source, settled) = settlement.finish(space, cancel);
                if let Some(body) = body {
                    let key = source.view().connection.key();
                    writer::release_egress(ctx.0.as_mut(), key, body.into_inner(), disposition);
                }
                if disposition == handler::Disposition::Closed {
                    true
                } else {
                    notify(source, ctx.2, *writes, ctx.0.as_mut(), turn.reborrow(), driver);
                    false
                }
            };
            if close || phase::finish(settlement.entry(&settled), ctx.0.as_ref()) {
                drop(writer::close(Ok(settlement.into_entry(&settled)), *writes, ctx, turn, driver));
            }
            (settled, ())
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
