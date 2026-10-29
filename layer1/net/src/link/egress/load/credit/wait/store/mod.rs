use std::{marker, mem, ptr};

use dope_core::driver::{self, schedule::ready};
pub(in crate::link::egress::load::credit::wait) use nodes::{NodeView, Nodes};
use o3::alloc;

use crate::link::egress::{
    create,
    load::credit::{
        self,
        wait::{store::bound::Bound, tree},
    },
    metadata,
};

mod bound;

pub(in crate::link::egress::load::credit) enum Activation<'p> {
    Ready,
    Stale(credit::Grant<'p>),
}

pub(super) struct Bypassable(pub(super) tree::Candidate);

pub(super) enum Bypass {
    Admitted(Bypassable),
    Exhausted,
}

mod nodes;

#[derive(Clone, Copy)]
pub(super) struct Waiting<'d> {
    pub(in crate::link::egress::load::credit::wait) key: ready::Retained<'d>,
    pub(in crate::link::egress::load::credit::wait) amount: credit::Queued,
    pub(in crate::link::egress::load::credit::wait) bypasses: usize,
}

struct Granted<'p, 'd> {
    key: ready::Retained<'d>,
    grant: credit::Grant<'p>,
}

enum Kind {
    Idle,
    Waiting,
    Granted,
}

union Payload<'p, 'd> {
    idle: (),
    waiting: mem::ManuallyDrop<Waiting<'d>>,
    granted: mem::ManuallyDrop<Granted<'p, 'd>>,
}

/// Releases the grant before its payload; the AVL plane is not a FIFO lane.
struct Record<'p, 'd> {
    kind: Kind,
    payload: Payload<'p, 'd>,
}

impl Drop for Record<'_, '_> {
    fn drop(&mut self) {
        if let Kind::Granted = self.kind {
            // SAFETY: Granted owns this initialized acquisition; taking it
            // through Ready clears the kind before any later record drop.
            let granted = unsafe { mem::ManuallyDrop::take(&mut self.payload.granted) };
            granted.grant.release();
        }
    }
}

pub(super) enum State<'a, 'p, 'd> {
    Idle(Idle<'a, 'p, 'd>),
    Waiting(Queued<'a, 'p, 'd>),
    Granted(Ready<'a, 'p, 'd>),
}

pub(super) struct Idle<'a, 'p, 'd>(&'a mut Record<'p, 'd>);

pub(super) struct Queued<'a, 'p, 'd> {
    record: ptr::NonNull<Record<'p, 'd>>,
    waiting: ptr::NonNull<Waiting<'d>>,
    borrow: marker::PhantomData<&'a mut Record<'p, 'd>>,
    index: tree::Index,
}

pub(super) struct Ready<'a, 'p, 'd> {
    record: ptr::NonNull<Record<'p, 'd>>,
    granted: ptr::NonNull<Granted<'p, 'd>>,
    borrow: marker::PhantomData<&'a mut Record<'p, 'd>>,
}

#[must_use]
pub(in crate::link::egress::load::credit) struct Selected<'a, 'pool, 'p, 'd> {
    record: &'a mut Record<'p, 'd>,
    scope: &'pool credit::Pool<'p, 'd, credit::Ready<'p, 'd>>,
    index: tree::Candidate,
    key: ready::Retained<'d>,
    amount: credit::Queued,
}

pub(super) struct Records<'a, 'p, 'd> {
    pointer: ptr::NonNull<Record<'p, 'd>>,
    borrow: marker::PhantomData<&'a mut [Record<'p, 'd>]>,
}

pub(super) struct Core<'p, 'd> {
    memory: alloc::raw::Block<Bound<'d, Record<'p, 'd>>, (), (), Record<'p, 'd>>,
}

impl<'p, 'd> Record<'p, 'd> {
    const fn idle() -> Self {
        Self {
            kind: Kind::Idle,
            payload: Payload { idle: () },
        }
    }
}

impl<'a, 'p, 'd> Idle<'a, 'p, 'd> {
    pub(in crate::link::egress::load::credit::wait) fn wait(
        self,
        index: tree::Index,
        waiting: Waiting<'d>,
    ) -> Queued<'a, 'p, 'd> {
        self.0.payload.waiting = mem::ManuallyDrop::new(waiting);
        self.0.kind = Kind::Waiting;
        Queued {
            record: ptr::NonNull::from(&mut *self.0),
            waiting: unsafe { ptr::NonNull::from(&mut *self.0.payload.waiting) },
            borrow: marker::PhantomData,
            index,
        }
    }
}

impl<'p, 'd> Queued<'_, 'p, 'd> {
    pub(in crate::link::egress::load::credit::wait) fn value(&self) -> Waiting<'d> {
        unsafe { *self.waiting.as_ptr() }
    }

    pub(in crate::link::egress::load::credit::wait) fn replace(
        mut self,
        waiting: Waiting<'d>,
    ) -> tree::Waiting {
        let current = unsafe { self.waiting.as_mut() };
        *current = waiting;
        tree::Waiting::new(self.index)
    }

    pub(in crate::link::egress::load::credit::wait) fn proof(self) -> tree::Waiting {
        tree::Waiting::new(self.index)
    }

    pub(in crate::link::egress::load::credit::wait) fn clear(mut self) -> tree::Waiting {
        unsafe { self.record.as_mut().kind = Kind::Idle };
        tree::Waiting::new(self.index)
    }
}
impl<'a, 'p, 'd> Ready<'a, 'p, 'd> {
    pub(in crate::link::egress::load::credit::wait) fn matches(
        &self,
        key: ready::Retained<'d>,
        amount: credit::Queued,
    ) -> bool {
        let granted = unsafe { self.granted.as_ref() };
        granted.key == key && granted.grant.amount() == amount.amount()
    }

    pub(in crate::link::egress::load::credit::wait) fn take(self) -> credit::Grant<'p> {
        self.vacate().1
    }

    pub(in crate::link::egress::load::credit::wait) fn vacate(
        mut self,
    ) -> (Idle<'a, 'p, 'd>, credit::Grant<'p>) {
        let granted = unsafe { self.granted.as_ptr().read() };
        unsafe { self.record.as_mut().kind = Kind::Idle };
        (Idle(unsafe { self.record.as_mut() }), granted.grant)
    }
}
impl<'pool, 'p, 'd> Selected<'_, 'pool, 'p, 'd> {
    fn lane(&self) -> credit::Lane<'pool, 'p, 'd, credit::Ready<'p, 'd>> {
        credit::Lane {
            pool: self.scope,
            index: metadata::Lane::at(self.index.position()),
        }
    }

    pub(in crate::link::egress::load::credit) fn activate(
        self,
        owner: driver::Reference<'d>,
    ) -> Activation<'p> {
        let lane = self.lane();
        let grant = lane.account().acquire(self.amount.amount());
        match owner.ready().try_activate(self.key) {
            Ok(()) => {
                self.record.payload.granted = mem::ManuallyDrop::new(Granted {
                    key: self.key,
                    grant,
                });
                self.record.kind = Kind::Granted;
                Activation::Ready
            }
            Err(ready::Stale) => {
                self.record.kind = Kind::Idle;
                Activation::Stale(grant)
            }
        }
    }
}

impl<'p, 'd> Core<'p, 'd> {
    pub(in crate::link::egress::load::credit::wait) fn try_new(
        lanes: metadata::LaneCount,
    ) -> Result<Self, create::Error> {
        let capacity = Bound::try_new(lanes).map_err(create::Error::Wait)?;
        let memory = alloc::raw::Block::try_from_fn(
            capacity,
            (),
            |_| Record::idle(),
            |_| tree::Node::idle(),
        )
        .map_err(alloc::Error::from)
        .map_err(create::Error::Allocation)?;
        Ok(Self { memory })
    }

    pub(in crate::link::egress::load::credit::wait) fn is_busy(
        &self,
        lane: credit::Lane<'_, 'p, 'd, credit::Ready<'p, 'd>>,
    ) -> bool {
        let index = tree::Index::from(lane);
        // SAFETY: Ready owns this table alongside the credit pool that admitted
        // the lane; construction initializes one record for every admitted lane.
        let record = unsafe { self.memory.as_ref().get_unchecked(index.offset()) };
        !matches!(record.kind, Kind::Idle)
    }

    pub(in crate::link::egress::load::credit::wait) fn parts(
        &mut self,
    ) -> (Records<'_, 'p, 'd>, Nodes<'_>) {
        let (_, records, nodes) = self.memory.parts_mut();
        (
            Records {
                pointer: ptr::NonNull::from(records).cast(),
                borrow: marker::PhantomData,
            },
            Nodes {
                pointer: ptr::NonNull::from(nodes).cast(),
                borrow: marker::PhantomData,
            },
        )
    }

    pub(in crate::link::egress::load::credit::wait) fn nodes(&self) -> NodeView<'_> {
        NodeView {
            pointer: ptr::NonNull::from(self.memory.tail()).cast(),
            len: self.memory.extent().len,
            borrow: marker::PhantomData,
        }
    }

    pub(in crate::link::egress::load::credit::wait) fn bypass(
        &self,
        index: tree::Candidate,
        limit: usize,
    ) -> Bypass {
        let record = unsafe { self.memory.as_ref().get_unchecked(index.offset()) };
        let waiting = unsafe { &*record.payload.waiting };
        if waiting.bypasses >= limit {
            return Bypass::Exhausted;
        }
        Bypass::Admitted(Bypassable(index))
    }
}

impl<'a, 'p, 'd> Records<'a, 'p, 'd> {
    pub(in crate::link::egress::load::credit::wait) fn state(
        &mut self,
        index: tree::Index,
    ) -> State<'_, 'p, 'd> {
        let state = unsafe { &mut *self.pointer.as_ptr().add(index.offset()) };
        let record = ptr::NonNull::from(&mut *state);
        match state.kind {
            Kind::Idle => State::Idle(Idle(state)),
            Kind::Waiting => State::Waiting(Queued {
                record,
                waiting: unsafe { ptr::NonNull::from(&mut *state.payload.waiting) },
                borrow: marker::PhantomData,
                index,
            }),
            Kind::Granted => State::Granted(Ready {
                record,
                granted: unsafe { ptr::NonNull::from(&mut *state.payload.granted) },
                borrow: marker::PhantomData,
            }),
        }
    }

    pub(in crate::link::egress::load::credit::wait) fn admit(&mut self, proof: Bypassable) {
        let record = unsafe { &mut *self.pointer.as_ptr().add(proof.0.offset()) };
        unsafe { &mut *record.payload.waiting }.bypasses += 1;
    }

    pub(in crate::link::egress::load::credit::wait) fn selected<'pool>(
        self,
        index: tree::Candidate,
        scope: &'pool credit::Pool<'p, 'd, credit::Ready<'p, 'd>>,
    ) -> (Selected<'a, 'pool, 'p, 'd>, tree::Waiting) {
        let record = unsafe { &mut *self.pointer.as_ptr().add(index.offset()) };
        let waiting = unsafe { *record.payload.waiting };
        let proof = tree::Waiting::new(index.index());
        (
            Selected {
                record,
                scope,
                index,
                key: waiting.key,
                amount: waiting.amount,
            },
            proof,
        )
    }
}

const _: () = {
    assert!(mem::size_of::<Core<'static, 'static>>() == 3 * mem::size_of::<usize>());
    assert!(
        mem::size_of::<Record<'static, 'static>>().is_multiple_of(mem::align_of::<tree::Node>())
    );
    assert!(
        Bound::<'_, Record<'static, 'static>>::stride()
            .is_multiple_of(mem::align_of::<Record<'static, 'static>>())
    );
    assert!(
        Bound::<'_, Record<'static, 'static>>::stride()
            .is_multiple_of(mem::align_of::<tree::Node>())
    );
};
