use std::time::Instant;

use dope::core::driver::schedule::clock;
use o3::base::time::{Nanos, Unix};

use crate::{
    config, host,
    lookup::{self, error},
    query::{answer::addr, family},
    storage::queries::{Entry, state},
};

pub(in crate::storage::queries) struct Query<'s, const N: usize> {
    pub(in crate::storage::queries) reason: lookup::Reason,
    pub(in crate::storage::queries) started_unix_ns: Unix<Nanos>,
    pub(in crate::storage::queries) started_at: Instant,
    pub(in crate::storage::queries) hard_deadline: Instant,
    branches: [state::Row<'s, N>; family::Version::COUNT],
}

impl<'s, const N: usize> Query<'s, N> {
    /// Uses the deadline admitted from this observation and configuration.
    pub(in crate::storage::queries) fn new<'d>(
        reason: lookup::Reason,
        unix: Unix<Nanos>,
        start: clock::Moment<'d>,
        hard: clock::Deadline<'d>,
        name: &host::Name,
        config: &'s config::Validated,
        entries: [Entry<'s>; 2],
    ) -> Self {
        let hard = hard.into_raw();
        let end = state::Retry::from_config(config).deadline(start.into_raw(), hard);
        let server = config.servers.first();
        let [four, six] = entries;
        Self {
            reason,
            started_unix_ns: unix,
            started_at: start.into_raw(),
            hard_deadline: hard,
            branches: [
                state::Row::new(four, state::Attempt::initial(name.clone(), end, server)),
                state::Row::new(six, state::Attempt::initial(name.clone(), end, server)),
            ],
        }
    }

    pub(in crate::storage::queries) fn entries(&self) -> [Entry<'s>; 2] {
        self.branches.each_ref().map(state::Row::entry)
    }

    pub(in crate::storage::queries) const fn branch_mut(
        &mut self,
        family: family::Version,
    ) -> &mut state::Row<'s, N> {
        &mut self.branches[family.index()]
    }

    pub(in crate::storage::queries) fn complete(
        &self,
        now: Instant,
    ) -> Option<Result<state::Answer<N>, error::Kind>> {
        if self.branches.iter().any(|row| {
            matches!(
                row.value,
                state::Branch::Pending(_) | state::Branch::Waiting { .. }
            )
        }) {
            return None;
        }
        let mut answer: Option<state::Answer<N>> = None;
        let mut cap = None;
        let mut failed: Option<error::Kind> = None;
        for branch in &self.branches {
            match &branch.value {
                state::Branch::Done {
                    addresses: values,
                    valid_until: until,
                } => {
                    let answer = answer.get_or_insert_with(|| state::Answer {
                        addresses: addr::Addresses::new(),
                        started_at: self.started_at,
                        completed_at: now,
                        valid_until: *until,
                    });
                    answer.valid_until = answer.valid_until.min(*until);
                    for &value in values.as_slice() {
                        match answer.addresses.insert(value) {
                            addr::Insert::Added | addr::Insert::Present => {}
                            addr::Insert::Full => {
                                cap = Some(answer.addresses.len() + 1);
                            }
                        }
                    }
                }
                state::Branch::Failed(kind)
                    if failed.is_none_or(|prior| kind.is_terminal() && !prior.is_terminal()) =>
                {
                    failed = Some(*kind);
                }
                state::Branch::Failed(_) | state::Branch::Empty => {}
                state::Branch::Pending(_) | state::Branch::Waiting { .. } => {
                    return None;
                }
            }
        }
        if let Some(actual) = cap {
            return Some(Err(error::Kind::Capacity { limit: N, actual }));
        }
        if let Some(kind) = failed {
            return Some(Err(kind));
        }
        Some(
            answer
                .filter(|answer| !answer.addresses.is_empty())
                .ok_or(error::Kind::Empty),
        )
    }

    pub(in crate::storage::queries) fn deadline(&self) -> Option<Instant> {
        self.branches
            .iter()
            .filter_map(|row| row.value.deadline())
            .min()
    }

    pub(in crate::storage::queries) fn has_stream(&self) -> bool {
        self.branches.iter().any(|row| row.value.is_stream())
    }
}
