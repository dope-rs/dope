use std::{mem, time::Instant};

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

pub(in crate::storage::queries) struct Seed<'name, 's> {
    reason: lookup::Reason,
    started_unix_ns: Unix<Nanos>,
    started_at: Instant,
    hard_deadline: Instant,
    end: Instant,
    name: &'name host::Name,
    server: config::Selected<'s>,
}

impl<'name, 's> Seed<'name, 's> {
    /// Uses the deadline admitted from this observation and configuration.
    pub(in crate::storage::queries) fn new<'d>(
        reason: lookup::Reason,
        unix: Unix<Nanos>,
        start: clock::Moment<'d>,
        hard: clock::Deadline<'d>,
        name: &'name host::Name,
        config: &'s config::Validated,
    ) -> Self {
        let hard = hard.into_raw();
        let end = state::Retry::from_config(config).deadline(start.into_raw(), hard);
        let server = config.servers.first();
        Self {
            reason,
            started_unix_ns: unix,
            started_at: start.into_raw(),
            hard_deadline: hard,
            end,
            name,
            server,
        }
    }

    pub(in crate::storage::queries) fn into_query<const N: usize>(
        self,
        entries: [Entry<'s>; 2],
    ) -> Query<'s, N> {
        let [four, six] = entries;
        Query {
            reason: self.reason,
            started_unix_ns: self.started_unix_ns,
            started_at: self.started_at,
            hard_deadline: self.hard_deadline,
            branches: [
                state::Row::new(
                    four,
                    state::Attempt::initial(self.name.clone(), self.end, self.server),
                ),
                state::Row::new(
                    six,
                    state::Attempt::initial(self.name.clone(), self.end, self.server),
                ),
            ],
        }
    }
}

impl<'s, const N: usize> Query<'s, N> {
    pub(in crate::storage::queries) fn entries(&self) -> [Entry<'s>; 2] {
        self.branches.each_ref().map(state::Row::entry)
    }

    pub(in crate::storage::queries) const fn branch_mut(
        &mut self,
        family: family::Version,
    ) -> &mut state::Row<'s, N> {
        &mut self.branches[family.index()]
    }

    pub(in crate::storage::queries) fn into_iter(self) -> impl Iterator<Item = state::Row<'s, N>> {
        self.branches.into_iter()
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

const _: () = {
    assert!(mem::size_of::<Seed<'static, 'static>>() == 88);
};
