//! Native source proofs for completion tables sharing one immutable extent.

use o3::{base::num::bounded, depot::queue::links};

use crate::driver::schedule::ready::{
    self,
    wake::{
        Once, Waker,
        resident::one,
        table::{Error, Registration, Sink, Table, storage},
    },
};

/// A joint source retaining the state that produces this table's indices.
/// # Safety
/// State and table share a fixed extent through `'target`; scope brands only admitted state indices.
pub unsafe trait Source<'a, 'target: 'a, 'd: 'target> {
    type State: 'a;

    fn parts(self) -> (&'a mut Self::State, &'target Table<'d>);
}

/// One completion admitted by the source of this exact invariant scope.
/// # Safety
/// Its index must lie in the paired table's fixed extent; the scope must not widen.
pub unsafe trait Index<'scope> {
    fn index(self) -> bounded::U32<0, { u32::MAX - 1 }>;

    fn wake(self, sink: &Sink<'scope, '_, '_>) -> Once
    where
        Self: Sized,
    {
        one(sink.table.driver, slot(self, sink).take_one())
    }

    /// Registers against the retained table without repeating extent admission.
    fn try_register<'target, 'd: 'target>(
        self,
        sink: &Sink<'scope, 'target, 'd>,
        wait: &mut Registration<'target, 'd>,
        wake: &Waker<'d>,
    ) -> Result<(), Error>
    where
        Self: Sized,
    {
        wait.try_keep(slot(self, sink), wake)
    }
}

fn slot<'scope, 'target, 'd>(
    index: impl Index<'scope>,
    sink: &Sink<'scope, 'target, 'd>,
) -> storage::Slot<'target, ready::Retained<'d>> {
    storage::Slot {
        table: &sink.table.inner,
        key: storage::Key(links::Index::from(index.index())),
    }
}
