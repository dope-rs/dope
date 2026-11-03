use std::time;

use crate::{client, conn, conn::id, runtime::brand};

pub(in crate::client::life) enum Retry {
    Dormant(client::AttemptState),
    Scheduled {
        next: client::AttemptState,
        at: time::Instant,
    },
}

impl Retry {
    pub(in crate::client::life) fn try_new<'scope, B: client::Backoff>(
        backoff: &'scope B,
        enabled: bool,
        scope: brand::Token<'scope>,
        domain: id::Domain,
        slot: client::SlotId,
        previous: client::AttemptState,
        restart: bool,
        seen: time::Instant,
    ) -> Result<Self, client::protocol::Failure> {
        if !enabled {
            return Ok(Self::Dormant(previous));
        }
        let attempt = previous.id(domain, slot);
        let next = previous
            .try_successor(restart)
            .map_err(|cause| client::protocol::Failure::Exhausted { attempt, cause })?;
        let delay = backoff.retry_after(next.retry_scope(scope, domain, slot));
        let at = conn::Timing::try_deadline(seen, delay)
            .map_err(|cause| client::protocol::Failure::Timing { attempt, cause })?;
        Ok(Self::Scheduled {
            next: next.state(),
            at,
        })
    }
}
