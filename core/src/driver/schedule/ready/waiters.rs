use std::{cell, marker, mem};

use o3::collections;

use crate::driver::{route::kind, schedule::ready};

const NONE: u32 = u32::MAX;

struct Link {
    previous: cell::Cell<u32>,
    next: cell::Cell<u32>,
    epoch: cell::Cell<u32>,
}

const _: () = assert!(mem::size_of::<Link>() == 3 * mem::size_of::<u32>());

pub(super) struct Waiters {
    links: Box<[Link]>,
    head: cell::Cell<u32>,
    tail: cell::Cell<u32>,
    _thread: o3::ThreadBound,
}

impl Waiters {
    pub(super) fn try_with_capacity(capacity: usize) -> Result<Self, collections::AllocationError> {
        use o3::collections::BoxSliceExt;

        Ok(Self {
            links: BoxSliceExt::try_box_with(capacity, |_| Link {
                previous: cell::Cell::new(NONE),
                next: cell::Cell::new(NONE),
                epoch: cell::Cell::new(0),
            })?,
            head: cell::Cell::new(NONE),
            tail: cell::Cell::new(NONE),
            _thread: o3::ThreadBound::NEW,
        })
    }

    pub(super) fn retire(&self, arena: &ready::Arena, key: ready::FixedKey<'_>) -> bool {
        let Some(resolved) = arena.entries.slots.resolve(key.key()) else {
            return false;
        };
        let Some(dispatch) = resolved.dispatch() else {
            return false;
        };
        match dispatch.get().kind() {
            kind::RECV_BUFFER_WAITING => {
                self.unlink(key);
                false
            }
            kind::RECV_BUFFER_GRANTED => true,
            _ => false,
        }
    }

    pub(super) fn wake(&self, arena: &ready::Arena) -> bool {
        while let Some((index, epoch)) = self.pop_front() {
            let key = ready::Key {
                index: index as u32,
                epoch,
                _arena: marker::PhantomData,
                _thread: o3::ThreadBound::NEW,
            };
            let Some(dispatch) = arena
                .entries
                .slots
                .resolve(key)
                .and_then(|entry| entry.dispatch())
            else {
                continue;
            };
            let target = dispatch.get();
            if target.kind() != kind::RECV_BUFFER_WAITING {
                continue;
            }
            dispatch.set(target.with_kind(kind::RECV_BUFFER_GRANTED));
            arena.ready.insert(index);
            return true;
        }
        false
    }

    pub(super) fn link(&self, key: ready::FixedKey<'_>) -> bool {
        let index = key.index() as usize;
        let Some(link) = self.links.get(index) else {
            return false;
        };
        if link.previous.get() != NONE {
            return false;
        }

        let index = index as u32;
        let tail = self.tail.get();
        link.previous.set(if tail == NONE { index } else { tail });
        link.next.set(NONE);
        link.epoch.set(key.epoch());
        if tail == NONE {
            self.head.set(index);
        } else {
            self.links[tail as usize].next.set(index);
        }
        self.tail.set(index);
        true
    }

    pub(super) fn unlink(&self, key: ready::FixedKey<'_>) -> bool {
        self.unlink_index(key.index() as usize)
    }

    pub(super) fn unlink_index(&self, index: usize) -> bool {
        let Some(link) = self.links.get(index) else {
            return false;
        };
        let previous = link.previous.replace(NONE);
        if previous == NONE {
            return false;
        }

        let next = link.next.replace(NONE);
        let index = index as u32;
        if previous == index {
            self.head.set(next);
        } else {
            self.links[previous as usize].next.set(next);
        }
        if next == NONE {
            self.tail
                .set(if previous == index { NONE } else { previous });
        } else if previous == index {
            self.links[next as usize].previous.set(next);
        } else {
            self.links[next as usize].previous.set(previous);
        }
        true
    }

    fn pop_front(&self) -> Option<(usize, u32)> {
        let index = self.head.get();
        if index == NONE {
            return None;
        }
        let epoch = self.links[index as usize].epoch.get();
        self.unlink_index(index as usize);
        Some((index as usize, epoch))
    }
}
