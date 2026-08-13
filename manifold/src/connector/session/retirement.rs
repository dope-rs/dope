use dope_core::driver;
use dope_net::{link::egress, wire};
use o3::buffer::storage;

use crate::connector::{
    self, app, codec, connection, lifecycle,
    session::{self, parser},
};

pub(super) trait Retirement<'d, const ID: u8, N: session::Session<'d, ID>, W: wire::Wire> {
    fn retire_connection<O>(
        &mut self,
        connection: connection::Ctx<'_, 'd, ID, W, session::Connection<'d, ID, N>, O>,
        egress: egress::Queue<'_, 'd, { connector::IOV_CAP }, N::Send>,
        reason: lifecycle::CloseReason,
        driver: &mut driver::Context<'_, 'd>,
    ) -> app::CloseOutcome;
}

impl<'d, const ID: u8, N: session::Retirement<'d, ID>, W: wire::Wire> Retirement<'d, ID, N, W>
    for session::Application<'d, ID, N, W>
{
    fn retire_connection<O>(
        &mut self,
        connection: connection::Ctx<'_, 'd, ID, W, session::Connection<'d, ID, N>, O>,
        egress: egress::Queue<'_, 'd, { connector::IOV_CAP }, N::Send>,
        mut reason: lifecycle::CloseReason,
        driver: &mut driver::Context<'_, 'd>,
    ) -> app::CloseOutcome {
        use connector::app::CloseOutcome;

        let (conn_id, conn, close_reason, work) = connection.into_parts();
        let finalized = conn.retirement_reason;
        if let Some(finalized) = finalized {
            reason = finalized;
        }
        let region = driver.region_token();
        let mut context = session::Ctx {
            conn_id,
            state: &mut conn.conn_state,
            sink: egress,
            region,
            close_reason,
        };
        self.session
            .begin_retirement(conn_id, reason, context.region);
        if finalized.is_some() {
            match self
                .session
                .retire_responses(conn_id, reason, work, context.region)
            {
                egress::ClearProgress::Done => {
                    self.session.disconnect(&mut context, reason);
                    return CloseOutcome::Complete(reason);
                }
                egress::ClearProgress::Retry | egress::ClearProgress::Waiting => {
                    return CloseOutcome::Yield;
                }
            }
        }
        let drained = parser::Parser::new(
            &mut self.session,
            &mut conn.ingress,
            &self.ingress_budget,
            &mut conn.parse_state,
            &mut context,
            work,
        )
        .run();
        match drained {
            parser::Outcome::Complete => {}
            parser::Outcome::Yield => return CloseOutcome::Yield,
            parser::Outcome::Capacity => {
                reason = lifecycle::CloseReason::Capacity;
                context.close_with(reason);
            }
            parser::Outcome::Overrun => {
                reason = lifecycle::CloseReason::Protocol;
                context.close_with(reason);
            }
            parser::Outcome::Close => {
                reason = lifecycle::CloseReason::Protocol;
                context.close_with(reason);
            }
        }
        match self.session.retire_requests(conn_id, work, context.region) {
            egress::ClearProgress::Done => {}
            egress::ClearProgress::Retry | egress::ClearProgress::Waiting => {
                return CloseOutcome::Yield;
            }
        }
        if <N::ConnState as lifecycle::Lifecycle>::wants_close(context.state).is_keep() {
            let remaining: wire::RetainedBytes<'d> = conn
                .ingress
                .snapshot()
                .map(wire::RetainedBytes::from)
                .unwrap_or_else(|| wire::RetainedBytes::from(storage::Shared::new()));
            match <N::Codec as codec::Codec>::finish(
                self.session.codec(),
                &mut conn.parse_state,
                remaining,
            ) {
                Ok(Some(head)) => self.session.response(head, &mut context),
                Ok(None) => {}
                Err(error) => {
                    self.session.protocol_error(error, &mut context);
                    reason = lifecycle::CloseReason::Protocol;
                }
            }
        }
        if !self.session.settle_responses(work, &mut context) {
            return CloseOutcome::Yield;
        }
        if let Some(protocol_reason) = *context.close_reason {
            reason = protocol_reason;
        }
        conn.retirement_reason = Some(reason);
        match self
            .session
            .retire_responses(conn_id, reason, work, context.region)
        {
            egress::ClearProgress::Done => {}
            egress::ClearProgress::Retry | egress::ClearProgress::Waiting => {
                return CloseOutcome::Yield;
            }
        }
        self.session.disconnect(&mut context, reason);
        CloseOutcome::Complete(reason)
    }
}
