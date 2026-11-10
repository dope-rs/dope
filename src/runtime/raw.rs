use std::num::NonZero;

use crate::conn::{id, wire::packet};

pub(super) struct Gso<'a> {
    batch: &'a mut packet::Gso,
    tail: *mut u8,
    participants: u8,
}

impl<'a> Gso<'a> {
    pub(super) fn try_new(
        batch: &'a mut packet::Gso,
        packets: usize,
        outer: usize,
        participants: usize,
    ) -> Result<Self, crate::conn::output::transmit::Admission> {
        let wire = (packets * outer).min(batch.limits().max_bytes().into_raw());
        let bytes = batch.try_prepare(wire + 1 + 16 * participants)?;
        let tail = bytes.as_mut_ptr().wrapping_add(bytes.capacity() - 1);
        Ok(Self {
            batch,
            tail,
            participants: 0,
        })
    }

    pub(super) fn append<I: Copy, const ID: u8, R>(
        &mut self,
        handle: id::Handle<I, ID>,
        path: NonZero<u64>,
        emit: impl FnOnce(&mut packet::Gso) -> R,
    ) -> R {
        let before = self.batch.packets();
        let result = emit(self.batch);
        if self.batch.packets() != before {
            self.participants += 1;
            // SAFETY: first reservation covers all selected contributors beyond
            // every temporary wire write; emission cannot move this allocation.
            unsafe {
                let entry = self.tail.sub(16 * usize::from(self.participants));
                entry.cast::<u32>().write_unaligned(handle.position().get());
                entry
                    .add(4)
                    .cast::<u32>()
                    .write_unaligned(handle.generation());
                entry.add(8).cast::<u64>().write_unaligned(path.get());
            }
        }
        result
    }

    pub(super) fn finish(self) -> Option<packet::Ready> {
        // SAFETY: this footer belongs to the first reserved, still-owned Vec.
        unsafe { self.tail.write(self.participants) };
        self.batch.take()
    }
}

pub(super) fn participants(
    bytes: &mut Vec<u8>,
) -> impl Iterator<Item = (NonZero<u32>, u32, NonZero<u64>)> + '_ {
    let end = bytes.as_mut_ptr().wrapping_add(bytes.capacity() - 1);
    // SAFETY: the private endpoint admits GSO only through Gso::finish;
    // native settlement returns that unchanged allocation and initialized footer.
    let count = unsafe { end.read() };
    (1..=count).map(move |index| unsafe {
        let entry = end.sub(16 * usize::from(index));
        (
            NonZero::new_unchecked(entry.cast::<u32>().read_unaligned()),
            entry.add(4).cast::<u32>().read_unaligned(),
            NonZero::new_unchecked(entry.add(8).cast::<u64>().read_unaligned()),
        )
    })
}
