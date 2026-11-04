use app::parser;
use dope_core::driver::{self, route::Identity};
use dope_net::{link::egress, wire};
use flow::Policy as _;
use parser::outcomes::Run;

use crate::client::{
    self,
    app::CloseOutcome,
    close, conn,
    conn::closing::retire,
    session::{self, app, response::flow, turns},
};

pub(in crate::client::session::app) fn run<'e, 'w, 'd: 'w, ID: Identity, N, W, O>(
    app: &mut app::State<'w, 'd, ID, N, W>,
    entry: conn::Ctx<'_, 'd, ID, W, session::Connection<'w, 'd, ID, N, W>, O>,
    egress: egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, N::Send>,
    mut reason: close::Reason,
    driver: &mut driver::Context<'_, 'd>,
) -> CloseOutcome
where
    N: session::Parser<'d, ID> + session::Retirement<'d, ID>,
    W: wire::Spec + 'd,
{
    let (id, conn, mut close, mut work, ready) = entry.into_retire();
    close.transmit_failed(|failure| {
        app.session
            .transmit_failed(id, failure, driver.region_token())
    });
    let close = 'settle: {
        let mut close = match close.enter(reason) {
            retire::Stage::Started(close) => {
                conn.ingress.discard(app.session.codec());
                close.quarantine(|current| {
                    reason = current;
                    app.session.quarantine(
                        id,
                        &mut conn.conn_state,
                        current,
                        driver.reborrow().into_regions(),
                    );
                })
            }
            retire::Stage::Retiring(close) => {
                reason = close.reason();
                conn.ingress.discard(app.session.codec());
                close
            }
            retire::Stage::Settled(close) => break 'settle close,
        };
        {
            if !matches!(reason, close::Reason::Remote) {
                // Cancellation may retire an EOF wait without admitting its input again.
                // Pending responses still require their owner's external wake.
                flow::Policy::cancel_eof(&mut conn.continuation);
            }
            let mut ctx = turns::Context::new(
                id,
                &mut conn.conn_state,
                egress.reborrow(),
                ready,
                driver.reborrow(),
                close.reborrow(),
            );
            let entry = if matches!(
                app.session.mode(id, ctx.state),
                session::ReceiveMode::Paused
            ) {
                flow::Entry::Paused
            } else {
                flow::Entry::Retire
            };
            let ended = match flow::Begin::begin(
                &mut conn.continuation,
                &mut app.session,
                &mut work,
                &mut ctx,
                entry,
            ) {
                start @ (flow::Start::Ready | flow::Start::Eof) => {
                    // Retiring close authority cannot reopen; no new input is decoded.
                    let ended = matches!(start, flow::Start::Eof);
                    conn.continuation.record(ended, flow::Record::Done);
                    ended
                }
                flow::Start::Idle | flow::Start::Finishing => false,
                flow::Start::Finished => true,
                flow::Start::Yield => return CloseOutcome::Yield,
                flow::Start::Blocked => return CloseOutcome::Blocked,
                flow::Start::Contract => {
                    reason = crate::app::receive::Failure::Contract.into();
                    ctx.request(close::Directive::reconnect(reason));
                    false
                }
                flow::Start::Close => {
                    reason = close::Reason::Protocol;
                    ctx.request(close::Directive::reconnect(reason));
                    false
                }
            };
            let remote = !ended
                && matches!(reason, close::Reason::Remote)
                && matches!(
                    app.session.mode(id, ctx.state),
                    session::ReceiveMode::Framed
                );
            if remote {
                let next = parser::Parser::new(
                    &mut app.session,
                    &mut conn.ingress,
                    app.ingress.budget(),
                    &mut ctx,
                )
                .finish(&mut conn.continuation, work);
                match next {
                    Run::Complete => {}
                    Run::Yield => return CloseOutcome::Yield,
                    Run::Blocked => return CloseOutcome::Blocked,
                    Run::Failed(cause) => {
                        reason = cause.into();
                        ctx.request(close::Directive::reconnect(reason));
                    }
                    Run::Close => {
                        reason = close::Reason::Protocol;
                        ctx.request(close::Directive::reconnect(reason));
                    }
                }
            }
            let step = {
                let mut turn = ctx.retirement(work);
                app.session.clear_send(id, &mut turn)
            };
            match step {
                egress::Cleared::Done => {}
                egress::Cleared::Retry => return CloseOutcome::Yield,
                egress::Cleared::Waiting => return CloseOutcome::Blocked,
            }
            if matches!(
                app.session.mode(id, ctx.state),
                session::ReceiveMode::Duplex
            ) {
                match app.session.finish(reason, &mut ctx) {
                    session::Settlement::Ready => {}
                    session::Settlement::Yield => return CloseOutcome::Yield,
                    session::Settlement::Blocked => return CloseOutcome::Blocked,
                }
            }
        }
        close.settle()
    };
    let reason = close.reason();
    let step = {
        let mut turn = turns::Retirement::new(work, driver.reborrow());
        app.session.clear_recv(id, reason, &mut turn)
    };
    match step {
        egress::Cleared::Done => {}
        egress::Cleared::Retry => return CloseOutcome::Yield,
        egress::Cleared::Waiting => return CloseOutcome::Blocked,
    }
    let policy = app
        .session
        .policy(id, &conn.conn_state, reason, driver.region_token());
    CloseOutcome::Complete(close::Directive::with_policy(reason, policy))
}
