use std::{mem, ops, time};

use crate::{
    lookup::error,
    query::answer::reply,
    storage::queries::{state, stream},
};

/// One logical DNS attempt. Transport bindings never replace its absolute
/// deadline; only a budget-consuming retry computes a new one.
pub(in crate::storage::queries) struct Attempt<'s> {
    request: state::Request<'s>,
    deadline: time::Instant,
}

impl<'s> Attempt<'s> {
    pub(in crate::storage::queries::state) fn initial(
        name: crate::host::Name,
        end: time::Instant,
        server: crate::config::Selected<'s>,
    ) -> Self {
        Self {
            request: state::Request::new(name, server),
            deadline: end,
        }
    }

    pub(in crate::storage::queries) const fn request(&self) -> &state::Request<'s> {
        &self.request
    }

    pub(in crate::storage::queries) const fn deadline(&self) -> time::Instant {
        self.deadline
    }

    pub(in crate::storage::queries) fn release(self, live: &mut stream::Live) {
        self.request.release(live);
    }

    pub(in crate::storage::queries) fn apply<const N: usize>(
        mut self,
        reply: reply::Reply<'_, N>,
        retry: state::Retry,
        hard: time::Instant,
        now: time::Instant,
        live: &mut stream::Live,
        row: &mut state::Row<'s, N>,
    ) {
        match reply {
            reply::Reply::Addresses {
                values,
                ttl,
                alias_hops: hops,
            } => {
                if let Err(cause) = self.request.hops(hops) {
                    return self.finish(
                        live,
                        state::Outcome::Failed(error::Kind::Malformed(cause)),
                        row,
                    );
                }
                let until = match self.request.try_expiry(now, ttl) {
                    Ok(until) => until,
                    Err(cause) => {
                        return self.finish(
                            live,
                            state::Outcome::Failed(error::Kind::Timing(cause)),
                            row,
                        );
                    }
                };
                let next = state::Outcome::Done {
                    addresses: values,
                    valid_until: until,
                };
                self.finish(live, next, row)
            }
            reply::Reply::Alias { name, ttl, hops } => {
                let name = match crate::host::Name::try_decode(name) {
                    Ok(name) => name,
                    Err(cause) => {
                        return self.finish(
                            live,
                            state::Outcome::Failed(error::Kind::Malformed(error::Malformed::Name(
                                cause,
                            ))),
                            row,
                        );
                    }
                };
                let until = match self.request.try_expiry(now, ttl) {
                    Ok(until) => until,
                    Err(cause) => {
                        return self.finish(
                            live,
                            state::Outcome::Failed(error::Kind::Timing(cause)),
                            row,
                        );
                    }
                };
                if let Err(cause) = self.request.try_alias(name, until, hops) {
                    self.release(live);
                    return row.finish(state::Outcome::Failed(error::Kind::Malformed(cause)));
                }
                self.deadline = retry.deadline(now, hard);
                row.pending(self)
            }
            reply::Reply::NoData => self.finish(live, state::Outcome::Empty, row),
            reply::Reply::Truncated => self.stream(live, row),
            reply::Reply::NameError => {
                self.finish(live, state::Outcome::Failed(error::Kind::Name), row)
            }
            reply::Reply::Refused => self.retry(retry, hard, now, live, error::Kind::Refused, row),
            reply::Reply::Server { code } => {
                self.retry(retry, hard, now, live, error::Kind::Server { code }, row)
            }
            reply::Reply::Capacity { actual } => self.finish(
                live,
                state::Outcome::Failed(error::Kind::Capacity { limit: N, actual }),
                row,
            ),
            reply::Reply::Malformed(cause) => self.finish(
                live,
                state::Outcome::Failed(error::Kind::Malformed(cause)),
                row,
            ),
        }
    }

    /// Retry one authenticated challenge with the latest accepted cookie pair,
    /// preserving the attempt's original deadline.
    pub(in crate::storage::queries) fn cookie<const N: usize>(
        mut self,
        live: &mut stream::Live,
        row: &mut state::Row<'s, N>,
    ) {
        match self.request.retry_cookie() {
            ops::ControlFlow::Continue(()) => row.pending(self),
            ops::ControlFlow::Break(()) => self.stream(live, row),
        }
    }

    pub(in crate::storage::queries) fn stream<const N: usize>(
        mut self,
        live: &mut stream::Live,
        row: &mut state::Row<'s, N>,
    ) {
        match self.request.start_stream(live) {
            ops::ControlFlow::Continue(()) => row.pending(self),
            ops::ControlFlow::Break(()) => {
                self.finish(live, state::Outcome::Failed(error::Kind::Truncated), row)
            }
        }
    }

    pub(in crate::storage::queries) fn retry<const N: usize>(
        mut self,
        retry: state::Retry,
        hard: time::Instant,
        now: time::Instant,
        live: &mut stream::Live,
        error: error::Kind,
        row: &mut state::Row<'s, N>,
    ) {
        if error.is_terminal() {
            return self.finish(live, state::Outcome::Failed(error), row);
        }
        if hard <= now {
            return self.finish(live, state::Outcome::Failed(error::Kind::Timeout), row);
        }
        if let Some(attempt) = self.request.attempt().next(retry.attempts) {
            self.request = self.request.retry(attempt, live);
            self.deadline = retry.deadline(now, hard);
            return row.pending(self);
        }
        self.finish(live, state::Outcome::Failed(error), row)
    }

    fn finish<const N: usize>(
        self,
        live: &mut stream::Live,
        next: state::Outcome<N>,
        row: &mut state::Row<'s, N>,
    ) {
        self.release(live);
        row.finish(next);
    }
}

const _: () = assert!(mem::size_of::<Attempt<'static>>() == 312);
