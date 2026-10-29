use o3::depot::{capacity, queue::round::set};

use crate::service::{
    self, pool,
    pool::members::{Record, Records, queues},
};

enum Vacant<'a, 'h, I, A> {
    Free(set::Vacant<'a, Record<I, A>, pool::Member, { u32::MAX }, false, capacity::Domain<'h>>),
    Reuse(set::Reuse<'a, Record<I, A>, pool::Member, { u32::MAX }, false, capacity::Domain<'h>>),
}

enum Entry<'a, 'h, I, A> {
    Keep {
        entry: set::Occupied<
            'a,
            Record<I, A>,
            pool::Member,
            { u32::MAX },
            false,
            capacity::Domain<'h>,
        >,
        value: service::Endpoint<I, A>,
    },
    New {
        entry: Vacant<'a, 'h, I, A>,
        value: service::Endpoint<I, A>,
    },
}

/// The exclusive records loan remains held by every reserved entry until commit or drop.
pub(super) struct Reservation<'a, 'h, I, A, const N: usize> {
    entries: [Option<Entry<'a, 'h, I, A>>; N],
    retained: usize,
}

impl<'a, 'h, I: Eq, A, const N: usize> Reservation<'a, 'h, I, A, N> {
    /// Every reserved slot remains behind this exclusive records borrow until commit or drop.
    /// No member operation can borrow the set until those guards have been consumed.
    pub(super) fn try_new(
        records: &'a mut Records<'h, I, A>,
        snapshot: service::Snapshot<service::Endpoint<I, A>, N>,
        preserve: bool,
    ) -> Result<Self, service::Reconcile> {
        let mut keep = [const { None }; N];
        let mut spare: [Option<Vacant<'a, 'h, I, A>>; N] = [const { None }; N];
        let mut space = spare.iter_mut();
        let mut retained = 0;
        for entry in records.entries() {
            let free = match entry {
                set::Entry::Vacant(entry) => Vacant::Free(entry),
                set::Entry::Occupied(entry) => {
                    if entry.get().is_desired()
                        && let Some((_, target)) = snapshot
                            .endpoints()
                            .iter()
                            .zip(keep.iter_mut())
                            .find(|pair| &entry.get().id == pair.0.id())
                    {
                        retained += usize::from(target.replace(entry).is_none());
                        continue;
                    }
                    if preserve && entry.get().health.owners != 0 {
                        continue;
                    }
                    match set::Reuse::try_from(entry) {
                        Ok(entry) => Vacant::Reuse(entry),
                        Err(_) => continue,
                    }
                }
            };
            if let Some(target) = space.next() {
                *target = Some(free);
            }
        }
        let limit = retained + (N - space.len());
        let actual = snapshot.endpoints().len();
        let mut spare = spare.into_iter().flatten();
        let mut entries = [const { None }; N];
        for (target, (value, prior)) in entries.iter_mut().zip(snapshot.into_iter().zip(keep)) {
            *target = Some(match prior {
                Some(entry) => Entry::Keep { entry, value },
                None => Entry::New {
                    entry: spare
                        .next()
                        .ok_or(service::Reconcile::Capacity { actual, limit })?,
                    value,
                },
            });
        }
        Ok(Self { entries, retained })
    }
}

impl<'h, I, A, const N: usize> Reservation<'_, 'h, I, A, N> {
    pub(super) fn commit(
        self,
        queue: &mut queues::Queue<'h, '_>,
    ) -> ([Option<pool::MemberKey<'h>>; N], usize) {
        let Self { entries, retained } = self;
        (
            entries.map(|entry| entry.map(|entry| entry.commit(queue))),
            retained,
        )
    }
}

impl<'h, I, A> Entry<'_, 'h, I, A> {
    pub(super) fn commit(self, queue: &mut queues::Queue<'h, '_>) -> pool::MemberKey<'h> {
        match self {
            Entry::Keep { mut entry, value } => {
                let key = pool::MemberKey(entry.key());
                let addr = value.into_parts().1;
                entry.get_mut().addr = addr;
                queues::Queue::reorder(entry);
                key
            }
            Entry::New { entry, value } => {
                let (id, addr) = value.into_parts();
                entry.insert(Record::new(id, addr), queue)
            }
        }
    }
}

impl<'h, I, A> Vacant<'_, 'h, I, A> {
    fn insert(self, value: Record<I, A>, queue: &mut queues::Queue<'h, '_>) -> pool::MemberKey<'h> {
        match self {
            Self::Free(entry) => {
                let entry = entry.insert(value);
                let member = pool::MemberKey(entry.key());
                entry.insert();
                member
            }
            Self::Reuse(entry) => {
                queue.retire(entry.key());
                let entry = entry.replace(value).0;
                let member = pool::MemberKey(entry.key());
                match entry.state() {
                    set::State::Idle(entry) => {
                        entry.insert();
                    }
                    set::State::Ready(entry) => {
                        entry.requeue();
                    }
                }
                member
            }
        }
    }
}
