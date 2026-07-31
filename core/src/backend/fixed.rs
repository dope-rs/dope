use std::io::{self, Error, ErrorKind};
use std::ops::Range;

use crate::driver::token::{SlotIndex, TokenCapacity};

pub(crate) struct FixedSlots {
    floor: u32,
    capacity: TokenCapacity,
    next: u32,
    free: Vec<Range<u32>>,
}

impl FixedSlots {
    pub(crate) fn new(floor: u32, ceiling: u32) -> io::Result<Self> {
        let capacity = TokenCapacity::new(ceiling as usize).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                "dope: fixed-file capacity exceeds token slots",
            )
        })?;
        if floor > ceiling {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "dope: fixed-file allocation floor exceeds capacity",
            ));
        }
        Ok(Self {
            floor,
            capacity,
            next: ceiling,
            free: Vec::new(),
        })
    }

    pub(crate) fn alloc_slot(&mut self) -> io::Result<SlotIndex> {
        let raw = self.alloc(1)?;
        let Some(slot) = self.capacity.slot(raw as usize) else {
            let _ = self.release(raw, 1);
            return Err(Error::new(
                ErrorKind::InvalidData,
                "dope: fixed-file allocator issued an unencodable slot",
            ));
        };
        Ok(slot)
    }

    pub(crate) fn alloc(&mut self, len: u32) -> io::Result<u32> {
        if len == 0 {
            return Ok(self.next);
        }
        if len == 1
            && let Some(range) = self.free.last_mut()
        {
            range.end -= 1;
            let base = range.end;
            if range.start == range.end {
                self.free.pop();
            }
            return Ok(base);
        }
        if let Some(index) = self
            .free
            .iter()
            .rposition(|range| range.end - range.start >= len)
        {
            let base = self.free[index].end - len;
            if base == self.free[index].start {
                if index + 1 == self.free.len() {
                    self.free.pop();
                } else {
                    self.free.remove(index);
                }
            } else {
                self.free[index].end = base;
            }
            return Ok(base);
        }
        let base = self
            .next
            .checked_sub(len)
            .filter(|&base| base >= self.floor)
            .ok_or_else(|| {
                Error::new(ErrorKind::OutOfMemory, "dope: fixed-file slots exhausted")
            })?;
        self.next = base;
        Ok(base)
    }

    pub(crate) fn release(&mut self, base: u32, len: u32) -> bool {
        if len == 0 {
            return true;
        }
        let Some((end, index)) = self.release_position(base, len) else {
            return false;
        };

        if base == self.next {
            self.next = end;
            while self
                .free
                .last()
                .is_some_and(|range| range.start == self.next)
            {
                let Some(range) = self.free.pop() else {
                    break;
                };
                self.next = range.end;
            }
            return true;
        }

        let mut merged = base..end;
        let mut index = index;
        if index > 0 && merged.end == self.free[index - 1].start {
            index -= 1;
            merged.end = self.free.remove(index).end;
        }
        if index < self.free.len() && self.free[index].end == merged.start {
            merged.start = self.free.remove(index).start;
        }
        self.free.insert(index, merged);
        true
    }

    fn release_position(&self, base: u32, len: u32) -> Option<(u32, usize)> {
        let end = base.checked_add(len)?;
        if base < self.next || end > self.capacity.get() as u32 {
            return None;
        }
        let index = self.free.partition_point(|range| range.start > base);
        if index > 0 && end > self.free[index - 1].start {
            return None;
        }
        if index < self.free.len() && self.free[index].end > base {
            return None;
        }
        Some((end, index))
    }
}
