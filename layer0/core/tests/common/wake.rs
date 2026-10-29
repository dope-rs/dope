use std::num;

use dope_core::driver::{
    self,
    route::{Erased, kind},
    schedule::{ready::wake::table, state::scope},
};
use o3::{base::num::bounded, depot::queue::resident};

pub const ONE_LANE: num::NonZero<usize> = num::NonZero::<usize>::MIN;
pub const TWO_LANES: num::NonZero<usize> = num::NonZero::<usize>::MIN.saturating_add(1);
pub const THREE_LANES: num::NonZero<usize> = num::NonZero::<usize>::MIN.saturating_add(2);
pub const FOUR_LANES: num::NonZero<usize> = num::NonZero::<usize>::MIN.saturating_add(3);
pub const MANY_LANES: num::NonZero<usize> = num::NonZero::<usize>::MIN.saturating_add(4_095);

pub trait Ready<'d> {
    fn ready_round(&self, activate: impl FnMut(Erased<'d, kind::Any>)) -> usize;
}
impl<'d> Ready<'d> for scope::Guard<'_, 'd> {
    fn ready_round(&self, activate: impl FnMut(Erased<'d, kind::Any>)) -> usize {
        self.ready()
            .drain(activate)
            .map_or(0, num::NonZero::<usize>::get)
    }
}

pub struct Completion<'d>(table::Table<'d>);

impl<'d> Completion<'d> {
    pub fn try_new(driver: driver::Reference<'d>) -> Result<Self, resident::Error> {
        Ok(Self(table::Table::try_new(driver, 2)?))
    }

    pub fn table(&self) -> &table::Table<'d> {
        &self.0
    }

    pub fn with<'target, R>(
        &'target self,
        run: impl for<'scope> FnOnce(Slot<'scope>, table::Sink<'scope, 'target, 'd>) -> R,
    ) -> R {
        let mut state = ();
        table::Table::with_source(
            Source {
                state: &mut state,
                table: self,
            },
            |_, scope, sink| {
                run(
                    Slot {
                        _scope: scope,
                        index: bounded::U32::exact::<1>(),
                    },
                    sink,
                )
            },
        )
    }
}

#[derive(Clone, Copy)]
pub struct Slot<'scope> {
    _scope: table::Scope<'scope>,
    index: bounded::U32<0, { u32::MAX - 1 }>,
}

struct Source<'a, 'target, 'd> {
    state: &'a mut (),
    table: &'target Completion<'d>,
}

// SAFETY: Completion owns exactly two slots for its full borrow. Its scoped
// callback admits only slot one and cannot replace the retained table.
unsafe impl<'a, 'target: 'a, 'd: 'target> table::raw::Source<'a, 'target, 'd>
    for Source<'a, 'target, 'd>
{
    type State = ();

    fn parts(self) -> (&'a mut Self::State, &'target table::Table<'d>) {
        (self.state, self.table.table())
    }
}

// SAFETY: only Completion::with constructs this index, branded by the source
// of its two-slot table. Index one stays in bounds for the retained table borrow.
unsafe impl<'scope> table::raw::Index<'scope> for Slot<'scope> {
    fn index(self) -> bounded::U32<0, { u32::MAX - 1 }> {
        self.index
    }
}
