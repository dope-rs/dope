use std::{mem::MaybeUninit, pin};

use dope_core::{
    driver::{self, route::{self, Identity}, schedule},
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
    connection.state.send.release(app.as_mut(), conn::Id::from_key(key), None, handler::Disposition::Sent);
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
    done: Completion<'d, impl route::Selection>,
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
    let _ = pool.settle(
        done, driver, &mut ctx,
        |_, _| {},
        |(app, _, _), key, body, _| {
            writer::release_payload(app.as_mut(), key, body, handler::Disposition::Sent);
        },
        |ctx, outcome, mut settlement, driver| {
            let connection = settlement.connection();
            let key = connection.key();
            // SAFETY: settlement retains this connection and its original deadline lane.
            let mut slot = Slot::from(unsafe { Proof::new(((connection, &mut *ctx.2), key)) });
            let mut body = MaybeUninit::uninit();
            let place = slot::Vacant::new(&mut body);
            let (mut source, settled);
            let close = 'complete: {
                let (disposition, body) = 'retire: {
                    let failure = match outcome {
                        SendOutcome::Pending => return (settlement.finish().1, ()),
                        SendOutcome::Direct { consumed } => {
                            if let Some(ended) = slot.state.send.settle(consumed, place) {
                                break 'retire ended;
                            }
                            slot.cancel_send();
                            (source, settled) = settlement.finish();
                            advance::<ID, A, _>(&mut source, ctx.2, *writes, ctx.0.as_mut(), turn.reborrow(), driver);
                            break 'complete false;
                        }
                        SendOutcome::Queued { .. } | SendOutcome::WriteShutdown => {
                            slot.cancel_send();
                            (source, settled) = settlement.finish();
                            notify(&mut source, ctx.2, *writes, ctx.0.as_mut(), turn.reborrow(), driver);
                            advance::<ID, A, _>(&mut source, ctx.2, *writes, ctx.0.as_mut(), turn.reborrow(), driver);
                            break 'complete false;
                        }
                        SendOutcome::Terminal => {
                            slot.cancel_send();
                            (source, settled) = settlement.finish();
                            sender::commit_end::<ID, A, _>(&mut source, ctx.2, ctx.0.as_mut(), driver);
                            break 'complete false;
                        }
                        SendOutcome::DirectClosed => None,
                        SendOutcome::Rejected { failure } => Some(conn::Transmit::Rejected(failure)),
                        SendOutcome::Failed { failure } => Some(conn::Transmit::Io(failure)),
                        SendOutcome::Invalid { error } => Some(conn::Transmit::Invalid(error)),
                        SendOutcome::Close => break 'retire (handler::Disposition::Closed, None),
                    };
                    (handler::Disposition::Closed, slot.state.send.take(failure, place))
                };
                slot.cancel_send();
                (source, settled) = settlement.finish();
                if let Some(body) = body {
                    ctx.0.as_mut().released(conn::Id::from_key(key), body.into_inner(), disposition);
                }
                if disposition == handler::Disposition::Closed {
                    true
                } else {
                    notify(&mut source, ctx.2, *writes, ctx.0.as_mut(), turn.reborrow(), driver);
                    false
                }
            };
            let mut entry = source.into_entry();
            if close || phase::finish(&mut entry, ctx.0.as_ref()) {
                drop(writer::close(Ok(entry), *writes, ctx, turn, driver));
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
