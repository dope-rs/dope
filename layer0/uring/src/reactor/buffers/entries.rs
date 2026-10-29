use std::{mem, num, ptr, sync::atomic};

use dope_sys::{
    io::Close,
    memory,
    memory::{
        Context as _, buffer,
        buffer::{Index as _, provided, provided::Context as _},
    },
};

use crate::reactor::{
    buffers::{self, Open},
    cq::Buffer,
    mappings,
};

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Entry {
    addr: u64,
    len: u32,
    bid: Buffer,
    tail: u16,
}

#[derive(Clone, Copy, Debug)]
struct View<L> {
    base: *const u8,
    layout: L,
}

#[derive(Debug)]
pub struct Producer<L: memory::raw::Receive> {
    entries: mappings::Mapping<Entry>,
    view: View<L>,
    tail: u16,
    published: u16,
}

impl<L: memory::raw::Receive> View<L> {
    fn address(self, slot: Buffer) -> u64 {
        self.base as u64 + slot.offset(self.layout.capacity()).get()
    }
}

impl<L: memory::raw::Receive> provided::Producer for Producer<L> {
    type Layout = L;
    type Context = buffers::raw::Context<L>;
    type Entry = Entry;
    type Error = Open<L>;

    fn new(context: &Self::Context) -> Result<Self, Self::Error> {
        let view = View {
            base: context.as_ptr(),
            layout: context.layout(),
        };
        let mut buffer = Buffer::ZERO;
        let entries = mappings::Mapping::new_with(view.layout.entries(), || {
            let bid = buffer;
            buffer.advance();
            Entry {
                addr: view.address(bid),
                len: view.layout.capacity().get(),
                bid,
                tail: 0,
            }
        })?;
        let tail = view.layout.entries().get();
        let producer = Self {
            entries,
            view,
            tail,
            published: tail,
        };
        producer.publish(tail);
        Ok(producer)
    }

    fn flush(&mut self) {
        if self.tail == self.published {
            return;
        }
        self.publish(self.tail);
        self.published = self.tail;
    }

    fn layout(&self) -> L {
        self.view.layout
    }

    fn registration(&self) -> (ptr::NonNull<Entry>, num::NonZero<u16>) {
        (self.entries.as_non_null(), self.view.layout.entries())
    }
}

impl<L: memory::raw::Receive> Producer<L> {
    pub(super) fn defer<T: buffer::raw::Token<Index = Buffer>>(&mut self, buffer: T) {
        let tail = self.tail;
        let position = tail & (self.view.layout.entries().get() - 1);
        let bid = buffer.index();
        // SAFETY: raw::Release alone supplies this producer's unique slot;
        // the layout masks the deferred position below the entry count.
        unsafe {
            let entry = self.entries.as_mut_ptr().add(usize::from(position));
            (&raw mut (*entry).addr).write(self.view.address(bid));
            (&raw mut (*entry).len).write(self.view.layout.capacity().get());
            (&raw mut (*entry).bid).write(bid);
        }
        drop(buffer);
        self.tail = tail.wrapping_add(1);
    }

    fn publish(&self, value: u16) {
        // SAFETY: the nonempty ring's first tail has AtomicU16 size/alignment.
        unsafe {
            use atomic::{AtomicU16, Ordering};

            let entry = self.entries.as_ptr().cast_mut();
            AtomicU16::from_ptr(&raw mut (*entry).tail).store(value, Ordering::Release);
        }
    }
}

impl<L: memory::raw::Receive> Close for Producer<L> {
    type State = <mappings::Mapping<Entry> as Close>::State;

    fn try_close(self) -> Result<(), Self::State> {
        self.entries.try_close()
    }
}

const _: () = {
    assert!(mem::size_of::<Entry>() == 16);
    assert!(mem::align_of::<Entry>() == 8);
    assert!(mem::offset_of!(Entry, addr) == 0);
    assert!(mem::offset_of!(Entry, len) == 8);
    assert!(mem::offset_of!(Entry, bid) == 12);
    assert!(mem::offset_of!(Entry, tail) == 14);
    assert!(mem::size_of::<atomic::AtomicU16>() == mem::size_of::<u16>());
    assert!(mem::align_of::<atomic::AtomicU16>() == mem::align_of::<u16>());
};
