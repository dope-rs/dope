use std::ops;

use dope_core::{
    driver::{
        route::Identity,
        schedule::{clock, ready},
    },
    io::socket,
};
use dope_net::link::pool;
use o3::base::time::Positive;

use crate::client::{
    conn::{
        self,
        engine::{self, liveness},
    },
    deadline::{self, book},
    side::mode::Mode,
};

type Source<'a, 'd, ID, P, X, C> =
    conn::Source<&'a mut liveness::State<'d, ID, P, X>, C, pool::Key<'d, ID>>;

#[must_use]
pub(in crate::client) struct Change<'a, 'd, ID: Identity, P, X, C>
where
    P: socket::Prepared,
    X: Mode<'d, ID>,
{
    source: Source<'a, 'd, ID, P, X, C>,
    target: ready::Target<'d>,
}

impl<'a, 'd, ID: Identity, P, X, C> Change<'a, 'd, ID, P, X, C>
where
    P: socket::Prepared,
    X: liveness::Schedule<'d, ID, P>,
{
    pub(in crate::client) fn closing(self, now: clock::Moment<'d>, window: Positive) -> C {
        self.edit(|slot| slot.closing(now, window))
    }

    pub(in crate::client) fn arm_after(
        self,
        kind: deadline::Fixed,
        now: clock::Moment<'d>,
        window: Positive,
    ) -> C {
        self.edit(|slot| slot.arm_after(kind, now, window))
    }
    pub(in crate::client) fn ensure_after(
        self,
        kind: deadline::Fixed,
        now: clock::Moment<'d>,
        window: Positive,
    ) -> C {
        self.edit(|slot| slot.ensure_after(kind, now, window))
    }
    pub(in crate::client) fn arm_at(self, kind: deadline::Fixed, at: clock::Deadline<'d>) -> C {
        self.edit(|slot| slot.arm_at(kind, at))
    }
    pub(in crate::client) fn cancel(self, kind: deadline::Kind) -> C {
        self.edit(|slot| slot.cancel(kind))
    }
    pub(in crate::client) fn clear(self) -> C {
        self.edit(|slot| slot.clear())
    }
    pub(in crate::client) fn arm_inbound(
        self,
        now: clock::Moment<'d>,
        seen: Option<clock::Moment<'d>>,
        window: Positive,
    ) -> C {
        self.edit(|slot| slot.arm_inbound(now, seen, window))
    }
    fn edit(self, edit: impl FnOnce(book::Slot<'_, 'd, ID, C>) -> C) -> C {
        let ((state, source), key) = self.source.into_inner();
        let before = X::earliest(state);
        // SAFETY: the complete incoming source is retained while projecting its primary book.
        let source = unsafe { engine::raw::reunite(&mut state.deadlines.book, source, key) };
        let source = edit(book::Slot::new(source));
        state.sync(before, self.target);
        source
    }
}

impl<'d, P: socket::Prepared, X: Mode<'d, ID>, C: ops::Deref, ID: Identity> ops::Deref
    for Change<'_, 'd, ID, P, X, C>
{
    type Target = C::Target;
    fn deref(&self) -> &Self::Target {
        self.source.source().1.deref()
    }
}
impl<'d, P: socket::Prepared, X: Mode<'d, ID>, C: ops::DerefMut, ID: Identity> ops::DerefMut
    for Change<'_, 'd, ID, P, X, C>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: native connection mutation cannot remove its slot or alter its paired heap plane.
        engine::raw::slot_mut(&mut self.source).deref_mut()
    }
}

impl<'a, 'd, ID: Identity, P, X, C> Change<'a, 'd, ID, P, X, C>
where
    P: socket::Prepared,
    X: Mode<'d, ID>,
{
    pub(in crate::client) fn new(
        source: Source<'a, 'd, ID, P, X, C>,
        target: ready::Target<'d>,
    ) -> Self {
        Self { source, target }
    }

    pub(in crate::client) fn into_source(self) -> C {
        self.source.into_inner().0.1
    }
}
