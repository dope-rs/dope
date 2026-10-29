use std::{marker, mem, num};

use dope_core::driver::{
    result,
    route::{Identity, KeyTag, table},
};
use o3::{
    alloc,
    buffer::pool,
    depot::{capacity, queue::ring},
};

use crate::network::datagram::{
    RejectCause,
    send::{self, owners},
};

pub(in crate::network::datagram) struct Pending<
    'd,
    ID: Identity,
    B = pool::Cursor,
    M: send::Mode = send::Plain,
> {
    queue: ring::Fifo<send::Queued<'d, B, M>>,
    series: Series,
    route: marker::PhantomData<fn() -> ID>,
}

pub(in crate::network::datagram) struct Back<'a, 'd, ID: Identity, B, M: send::Mode> {
    entry: ring::Vacant<'a, ring::Fifo<send::Queued<'d, B, M>>>,
    owner: owners::Queued<'d, ID>,
}

/// Borrows the initialized head until submission or retirement consumes it.
pub(in crate::network::datagram) struct Front<'a, 'd, ID: Identity, B, M: send::Mode> {
    entry: ring::Front<'a, ring::Fifo<send::Queued<'d, B, M>>>,
    owner: owners::Queued<'d, ID>,
    series: &'a mut Series,
}

pub(in crate::network::datagram) enum Commit<T> {
    Submitted(T),
    Deferred(result::SubmitError),
}

struct Series(Option<num::NonZero<u64>>);

impl<'d, ID: Identity, B, M: send::Mode> Pending<'d, ID, B, M> {
    pub(in crate::network::datagram) fn try_new(
        capacity: table::Capacity,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            queue: ring::Fifo::try_new(capacity.count())?,
            series: Series(Some(num::NonZero::<u64>::MIN)),
            route: marker::PhantomData,
        })
    }

    pub(in crate::network::datagram) fn back(
        &mut self,
    ) -> Result<Back<'_, 'd, ID, B, M>, RejectCause> {
        let queued = self.queue.count();
        let owner = self
            .series
            .back(queued)
            .ok_or(RejectCause::IdentityExhausted)?;
        let entry = self.queue.vacant().ok_or(RejectCause::QueueFull)?;
        Ok(Back { entry, owner })
    }

    pub(in crate::network::datagram) fn front(&mut self) -> Option<Front<'_, 'd, ID, B, M>> {
        let Self { queue, series, .. } = self;
        let owner = series.front()?;
        let entry = queue.front_entry()?;
        Some(Front {
            entry,
            owner,
            series,
        })
    }

    pub(in crate::network::datagram) fn pop(
        &mut self,
    ) -> Option<(owners::Queued<'d, ID>, send::Queued<'d, B, M>)> {
        Some(self.front()?.pop())
    }

    pub(in crate::network::datagram) fn clear(&mut self) {
        self.series.advance_by(self.queue.count().raw());
        self.queue.clear();
    }

    pub(in crate::network::datagram) fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

impl<'d, ID: Identity, B, M: send::Mode> Back<'_, 'd, ID, B, M> {
    pub(in crate::network::datagram) fn push(
        self,
        value: send::Queued<'d, B, M>,
    ) -> owners::Queued<'d, ID> {
        self.entry.push_back(value);
        self.owner
    }
}

impl<'d, ID: Identity, B, M: send::Mode> Front<'_, 'd, ID, B, M> {
    pub(in crate::network::datagram) fn submit<R>(
        self,
        commit: impl FnOnce(
            send::Queued<'d, B, M>,
        ) -> Result<R, (result::SubmitError, send::Queued<'d, B, M>)>,
    ) -> Commit<(owners::Queued<'d, ID>, R)> {
        let (value, vacancy) = self.entry.take();
        match commit(value) {
            Ok(value) => {
                self.series.advance_by(1);
                Commit::Submitted((self.owner, value))
            }
            Err((error, value)) => {
                vacancy.push_front(value);
                Commit::Deferred(error)
            }
        }
    }

    fn pop(self) -> (owners::Queued<'d, ID>, send::Queued<'d, B, M>) {
        self.series.advance_by(1);
        (self.owner, self.entry.remove())
    }
}

impl Series {
    fn front<'d, ID: Identity>(&self) -> Option<owners::Queued<'d, ID>> {
        Some(owners::Queued::new(self.0?))
    }

    fn back<'d, ID: Identity>(&self, queued: capacity::Count) -> Option<owners::Queued<'d, ID>> {
        let serial = self.0?.checked_add(u64::from(queued.raw()))?;
        Some(owners::Queued::new(serial))
    }

    fn advance_by(&mut self, count: u32) {
        self.0 = self
            .0
            .and_then(|serial| serial.checked_add(u64::from(count)));
    }
}

const _: () = {
    assert!(mem::size_of::<Series>() == mem::size_of::<u64>());
    assert!(mem::size_of::<Commit<()>>() == 1);
    assert!(mem::align_of::<Commit<()>>() == 1);
    assert!(
        mem::size_of::<
            Commit<(
                owners::Queued<'static, KeyTag<0>>,
                owners::Flight<'static, KeyTag<0>>
            )>,
        >() == mem::size_of::<(
            owners::Queued<'static, KeyTag<0>>,
            owners::Flight<'static, KeyTag<0>>
        )>()
    );
    assert!(
        mem::align_of::<
            Commit<(
                owners::Queued<'static, KeyTag<0>>,
                owners::Flight<'static, KeyTag<0>>
            )>,
        >() == mem::align_of::<(
            owners::Queued<'static, KeyTag<0>>,
            owners::Flight<'static, KeyTag<0>>
        )>()
    );
    assert!(
        mem::size_of::<Pending<'static, KeyTag<0>>>()
            == mem::size_of::<ring::Fifo<send::Queued<'static>>>() + mem::size_of::<u64>()
    );
};
