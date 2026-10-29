use app::parser;
use dope_core::driver::{
    self,
    route::Identity,
    schedule::{app::Work, ready},
};
use dope_net::{link::egress, wire};
use parser::outcomes::Run;

use crate::client::{
    self,
    app::CloseOutcome,
    close, conn,
    conn::closing::retire,
    session::{self, app, response::flow, turns},
};

pub(in crate::client::session::app) struct State<
    'app,
    'conn,
    'w,
    'd: 'w,
    ID: Identity,
    N: session::Parser<'d, ID>,
    W: wire::Spec + 'd,
> {
    app: &'app mut app::State<'w, 'd, ID, N, W>,
    id: conn::Id<'d, ID>,
    conn: &'conn mut session::Connection<'w, 'd, ID, N, W>,
    close: retire::Retire<'conn, 'd, ID>,
    work: Work<'conn, 'd>,
    ready: ready::Target<'d>,
}

impl<'app, 'conn, 'w, 'd: 'w, ID: Identity, N, W> State<'app, 'conn, 'w, 'd, ID, N, W>
where
    'd: 'app + 'conn,
    N: session::Parser<'d, ID> + session::Retirement<'d, ID>,
    W: wire::Spec + 'd,
{
    pub(in crate::client::session::app) const fn new(
        app: &'app mut app::State<'w, 'd, ID, N, W>,
        id: conn::Id<'d, ID>,
        conn: &'conn mut session::Connection<'w, 'd, ID, N, W>,
        close: retire::Retire<'conn, 'd, ID>,
        work: Work<'conn, 'd>,
        ready: ready::Target<'d>,
    ) -> Self {
        Self {
            app,
            id,
            conn,
            close,
            work,
            ready,
        }
    }

    pub(in crate::client::session::app) fn run<'e>(
        self,
        egress: egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, N::Send>,
        mut reason: close::Reason,
        driver: &mut driver::Context<'_, 'd>,
    ) -> CloseOutcome {
        let Self {
            app,
            id,
            conn,
            close,
            work,
            ready,
        } = self;
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
                retire::Stage::Finalized => return CloseOutcome::Yield,
                retire::Stage::Claimed => return CloseOutcome::Yield,
            };
            {
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
                let retired = parser::Parser::new(
                    &mut app.session,
                    &mut conn.ingress,
                    app.ingress.budget(),
                    &mut ctx,
                    work,
                )
                .retire(&mut conn.continuation, entry);
                let (next, ended) = match retired {
                    parser::Retire::Run { next, ended } => (next, ended),
                    parser::Retire::Finishing => (Run::Complete, false),
                    parser::Retire::Finished => (Run::Complete, true),
                };
                match next {
                    Run::Complete => {}
                    Run::Yield | Run::Blocked => return CloseOutcome::Yield,
                    Run::Failed(cause) => {
                        reason = cause.into();
                        ctx.request(close::Directive::reconnect(reason));
                    }
                    Run::Close => {
                        reason = close::Reason::Protocol;
                        ctx.request(close::Directive::reconnect(reason));
                    }
                }
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
                        work,
                    )
                    .finish(&mut conn.continuation);
                    match next {
                        Run::Complete => {}
                        Run::Yield | Run::Blocked => return CloseOutcome::Yield,
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
                    egress::Cleared::Retry | egress::Cleared::Waiting => {
                        return CloseOutcome::Yield;
                    }
                }
                if matches!(
                    app.session.mode(id, ctx.state),
                    session::ReceiveMode::Duplex
                ) {
                    match app.session.finish(reason, &mut ctx) {
                        session::Settlement::Ready => {}
                        session::Settlement::Yield | session::Settlement::Blocked => {
                            return CloseOutcome::Yield;
                        }
                    }
                }
            }
            reason = close.reason();
            close.settle(reason)
        };
        let reason = close.reason();
        let step = {
            let mut turn = turns::Retirement::new(work, driver.reborrow());
            app.session.clear_recv(id, reason, &mut turn)
        };
        match step {
            egress::Cleared::Done => {}
            egress::Cleared::Retry | egress::Cleared::Waiting => {
                return CloseOutcome::Yield;
            }
        }
        let policy = app
            .session
            .policy(id, &conn.conn_state, reason, driver.region_token());
        CloseOutcome::Complete(close::Directive::with_policy(reason, policy))
    }
}
