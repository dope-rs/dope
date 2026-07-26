
use crate::driver::token::SlotIndex;
use crate::io::fd::FdSlot;
use o3::collections::FixedQueue;
use crate::backend::uring::sqe::Create;
use crate::backend::uring::sqe::Sqe;
use std::mem::replace;

#[derive(Clone, Copy)]
enum FileState {
    Empty,
    Live,
    Creating { user_data: u64, close_pending: bool },
    Closing,
}

pub(crate) enum Admission {
    Start,
    Defer,
    Reject,
}

pub(crate) struct FileTable {
    state: Box<[FileState]>,
    pending: Box<[Option<(Create, Sqe)>]>,
    ready: FixedQueue<FdSlot>,
    deferred_close: FixedQueue<FdSlot>,
}

impl FileTable {
    pub(super) fn new(slots: usize) -> Self {
        Self {
            state: (0..slots).map(|_| FileState::Empty).collect(),
            pending: (0..slots).map(|_| None).collect(),
            ready: FixedQueue::with_capacity(slots),
            deferred_close: FixedQueue::with_capacity(slots),
        }
    }

    pub(crate) fn admission(&self, slot: FdSlot) -> Admission {
        let index = slot.raw() as usize;
        let Some(state) = self.state.get(index).copied() else {
            return Admission::Reject;
        };
        match state {
            FileState::Empty => Admission::Start,
            FileState::Closing if self.pending[index].is_none() => Admission::Defer,
            FileState::Creating {
                close_pending: true,
                ..
            } if self.pending[index].is_none() => Admission::Defer,
            FileState::Live | FileState::Creating { .. } | FileState::Closing => Admission::Reject,
        }
    }

    pub(crate) fn begin_create(&mut self, create: Create) {
        debug_assert!(matches!(
            self.state[create.slot.raw() as usize],
            FileState::Empty
        ));
        self.state[create.slot.raw() as usize] = FileState::Creating {
            user_data: create.user_data,
            close_pending: false,
        };
    }

    pub(crate) fn defer_create(&mut self, create: Create, sqe: Sqe) {
        self.pending[create.slot.raw() as usize] = Some((create, sqe));
    }

    pub(crate) fn set_live(&mut self, slot: FdSlot) {
        self.state[slot.raw() as usize] = FileState::Live;
    }

    pub(super) fn complete_create(&mut self, slot: SlotIndex, result: i32) -> Option<u64> {
        let index = slot.raw() as usize;
        let state = self.state.get_mut(index)?;
        let FileState::Creating {
            user_data,
            close_pending,
        } = replace(state, FileState::Empty)
        else {
            return None;
        };
        if result >= 0 {
            if close_pending {
                *state = FileState::Closing;
                let Some(entry) = self.deferred_close.vacant_entry() else {
                    unreachable!()
                };
                entry.push_back(FdSlot::new(index as u32));
            } else {
                *state = FileState::Live;
            }
        } else if self.pending[index].is_some() {
            let Some(entry) = self.ready.vacant_entry() else {
                unreachable!()
            };
            entry.push_back(FdSlot::new(index as u32));
        }
        Some(user_data)
    }

    pub(super) fn complete_close(&mut self, slot: SlotIndex) {
        let index = slot.raw() as usize;
        let Some(state) = self.state.get_mut(index) else {
            return;
        };
        debug_assert!(matches!(*state, FileState::Closing));
        *state = FileState::Empty;
        if self.pending[index].is_some() {
            let Some(entry) = self.ready.vacant_entry() else {
                unreachable!()
            };
            entry.push_back(FdSlot::new(index as u32));
        }
    }

    pub(super) fn mark_accepted(&mut self, result: i32, close_pending: bool) {
        if result < 0 {
            return;
        }
        if let Some(state) = self.state.get_mut(result as usize) {
            debug_assert!(matches!(*state, FileState::Empty));
            if close_pending {
                *state = FileState::Closing;
                let Some(entry) = self.deferred_close.vacant_entry() else {
                    unreachable!()
                };
                entry.push_back(FdSlot::new(result as u32));
            } else {
                *state = FileState::Live;
            }
        }
    }

    pub(super) fn flush_ready(&mut self, mut push: impl FnMut(&Sqe) -> bool) {
        while let Some(&slot) = self.ready.front() {
            let index = slot.raw() as usize;
            let Some((create, sqe)) = self.pending[index].take() else {
                self.ready.pop_front();
                continue;
            };
            if !push(&sqe) {
                self.pending[index] = Some((create, sqe));
                break;
            }
            self.begin_create(create);
            self.ready.pop_front();
        }
    }

    pub(super) fn flush_deferred_close(&mut self, mut push_close: impl FnMut(FdSlot) -> bool) {
        while let Some(&slot) = self.deferred_close.front() {
            if !push_close(slot) {
                break;
            }
            self.deferred_close.pop_front();
        }
    }

    pub(super) fn release(&mut self, slot: FdSlot, mut push_close: impl FnMut(FdSlot) -> bool) {
        let index = slot.raw() as usize;
        let Some(state) = self.state.get(index).copied() else {
            return;
        };
        match state {
            FileState::Empty | FileState::Closing => {
                self.pending[index] = None;
            }
            FileState::Live => {
                self.state[index] = FileState::Closing;
                self.flush_deferred_close(&mut push_close);
                if !push_close(slot) {
                    let Some(entry) = self.deferred_close.vacant_entry() else {
                        unreachable!()
                    };
                    entry.push_back(slot);
                }
            }
            FileState::Creating {
                user_data,
                close_pending: false,
            } => {
                self.state[index] = FileState::Creating {
                    user_data,
                    close_pending: true,
                };
            }
            FileState::Creating {
                close_pending: true,
                ..
            } => {
                self.pending[index] = None;
            }
        }
    }
}
