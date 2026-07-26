use o3::cell::RawCell;

use crate::io::RecvBuffer;

use super::NONE;
use std::mem::replace;

enum SlotState<'d> {
    Free {
        next: u32,
    },
    Reserved,
    Queued {
        value: RecvBuffer<'d>,
        len: u32,
        next: u32,
    },
}

pub(super) enum Recycle {
    Free { next: u32 },
    Reserved,
}

pub(super) struct RecvSlot<'d> {
    state: RawCell<SlotState<'d>>,
}

impl<'d> RecvSlot<'d> {
    pub(super) fn free(next: u32) -> Self {
        Self {
            state: RawCell::new(SlotState::Free { next }),
        }
    }

    pub(super) fn claim(&self) -> Option<u32> {
        unsafe {
            self.state.with_mut(|state| {
                let previous = replace(state, SlotState::Reserved);
                match previous {
                    SlotState::Free { next } => Some(next),
                    other => {
                        *state = other;
                        None
                    }
                }
            })
        }
    }

    pub(super) fn is_reserved(&self) -> bool {
        unsafe {
            self.state
                .with(|state| matches!(state, SlotState::Reserved))
        }
    }

    pub(super) fn release(&self, next: u32) -> bool {
        unsafe {
            self.state.with_mut(|state| {
                if !matches!(state, SlotState::Reserved) {
                    return false;
                }
                *state = SlotState::Free { next };
                true
            })
        }
    }

    pub(super) fn insert(&self, value: RecvBuffer<'d>, len: u32) -> Result<(), RecvBuffer<'d>> {
        unsafe {
            self.state.with_mut(|state| match state {
                SlotState::Reserved => {
                    *state = SlotState::Queued {
                        value,
                        len,
                        next: NONE,
                    };
                    Ok(())
                }
                _ => Err(value),
            })
        }
    }

    pub(super) fn set_queued_next(&self, next: u32) -> bool {
        unsafe {
            self.state.with_mut(|state| match state {
                SlotState::Queued {
                    next: queued_next, ..
                } => {
                    *queued_next = next;
                    true
                }
                _ => false,
            })
        }
    }

    pub(super) fn queued_meta(&self) -> Option<(usize, u32)> {
        unsafe {
            self.state.with(|state| match state {
                SlotState::Queued { len, next, .. } => Some((*len as usize, *next)),
                _ => None,
            })
        }
    }

    pub(super) fn copy_prefix(&self, dst: &mut [u8]) -> bool {
        unsafe {
            self.state.with(|state| match state {
                SlotState::Queued { value, len, .. } if dst.len() <= *len as usize => {
                    dst.copy_from_slice(&value.as_slice()[..dst.len()]);
                    true
                }
                _ => false,
            })
        }
    }

    pub(super) fn advance(&self, n: usize) -> bool {
        unsafe {
            self.state.with_mut(|state| match state {
                SlotState::Queued { value, len, .. } if n < *len as usize => {
                    value.advance(n);
                    *len -= n as u32;
                    true
                }
                _ => false,
            })
        }
    }

    pub(super) fn take(&self, recycle: Recycle) -> Option<RecvBuffer<'d>> {
        unsafe {
            self.state.with_mut(|state| {
                let next_state = match recycle {
                    Recycle::Free { next } => SlotState::Free { next },
                    Recycle::Reserved => SlotState::Reserved,
                };
                let previous = replace(state, next_state);
                match previous {
                    SlotState::Queued { value, .. } => Some(value),
                    other => {
                        *state = other;
                        None
                    }
                }
            })
        }
    }
}
