use crate::storage::queries::{Entry, ledger, state};

pub(in crate::storage::queries) struct Row<'s, const N: usize> {
    pub(in crate::storage::queries) value: state::Branch<'s, N>,
    entry: Entry<'s>,
}

impl<'s, const N: usize> Row<'s, N> {
    pub(in crate::storage::queries::state) fn entry(&self) -> Entry<'s> {
        self.entry
    }

    pub(in crate::storage::queries::state) fn new(
        entry: Entry<'s>,
        probe: state::Attempt<'s>,
    ) -> Self {
        entry.set(ledger::Pending::id(probe.request().stream()));
        Self {
            entry,
            value: state::Branch::Pending(probe),
        }
    }

    pub(in crate::storage::queries) fn register(&self, request: &state::Request<'s>) {
        self.entry.set(ledger::Pending::id(request.stream()));
    }

    pub(in crate::storage::queries) fn unregister(&self, request: &state::Request<'s>) {
        self.entry.clear(ledger::Pending::id(request.stream()));
    }

    pub(in crate::storage::queries) fn pending(&mut self, probe: state::Attempt<'s>) {
        self.register(probe.request());
        self.value = state::Branch::Pending(probe);
    }

    pub(super) fn finish(&mut self, outcome: state::Outcome<N>) {
        self.value = match outcome {
            state::Outcome::Done {
                addresses: addrs,
                valid_until: until,
            } => state::Branch::Done {
                addresses: addrs,
                valid_until: until,
            },
            state::Outcome::Empty => state::Branch::Empty,
            state::Outcome::Failed(error) => state::Branch::Failed(error),
        };
    }
}
