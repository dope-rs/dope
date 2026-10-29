use core::{mem, num::NonZero};

use dope_sys::io::socket::{self, msg, msg::Header};
use o3::{
    buffer,
    depot::array::{self, copy},
};

use crate::io::transfer;

pub trait Iovec {
    /// Constructs a retained iovec.
    /// # Safety
    /// The bytes stay fixed through terminal completion or quiescence.
    unsafe fn retain(self) -> msg::Iovec;
}

impl Iovec for &[u8] {
    unsafe fn retain(self) -> msg::Iovec {
        msg::Iovec::from_slice(self)
    }
}

pub trait Vectored<'a, const N: usize = { super::MAX_IOVECS }> {
    /// Constructs a retained descriptor graph.
    /// # Safety
    /// Inputs stay fixed, count fits `N` and `MAX_IOVECS`, and `bytes` is the exact sum.
    unsafe fn retain(self) -> super::Vectored<'a, N>;
}

impl<'a, const N: usize> Vectored<'a, N>
    for (&'a mut Header, &'a copy::Vec<msg::Iovec, N>, transfer::Len)
{
    unsafe fn retain(self) -> super::Vectored<'a, N> {
        const { assert!(N <= super::MAX_IOVECS) };
        let (header, iovecs, bytes) = self;
        super::Vectored::from_parts(header, iovecs.as_slice(), bytes)
    }
}

impl<'a, const N: usize> Vectored<'a, N>
    for (
        &'a mut Header,
        &'a [msg::Iovec; N],
        array::Len<N>,
        transfer::Len,
    )
{
    unsafe fn retain(self) -> super::Vectored<'a, N> {
        const { assert!(N <= super::MAX_IOVECS) };
        let (header, iovecs, count, bytes) = self;
        super::Vectored::from_parts(header, count.prefix(iovecs), bytes)
    }
}

pub(super) trait Project<'a> {
    fn slice(self) -> &'a [u8];
}

impl<'a> Project<'a> for &'a msg::Iovec {
    fn slice(self) -> &'a [u8] {
        // SAFETY: construction proves the bytes live for the graph lifetime.
        unsafe { socket::raw::Iovec::as_slice(self) }
    }
}

/// Compact exact cursor over one retained iovec graph.
#[derive(Clone, Copy)]
pub struct Cursor<'a> {
    iovecs: &'a [msg::Iovec],
    index: u32,
    local: u32,
    remaining: transfer::Len,
}

impl<'a> Cursor<'a> {
    pub const fn bytes(&self) -> transfer::Len {
        self.remaining
    }

    fn start<const N: usize>(source: &super::Vectored<'a, N>) -> Self {
        Self {
            iovecs: source.iovs,
            index: 0,
            local: 0,
            remaining: source.bytes(),
        }
    }

    fn new<const N: usize>(source: &super::Vectored<'a, N>, mut offset: usize) -> Option<Self> {
        let len = source.bytes().into_raw();
        if offset > len {
            return None;
        }
        let left = transfer::Len::prefix(len - offset, source.bytes());
        let iovecs: &'a [msg::Iovec] = source.iovs;
        let mut index = 0;
        let mut local = 0_usize;
        while let Some(iovec) = iovecs.get(index as usize) {
            if offset < iovec.len() {
                local = offset;
                break;
            }
            offset -= iovec.len();
            index += 1;
        }
        Some(Self {
            iovecs,
            index,
            local: local as u32,
            remaining: left,
        })
    }
}

// SAFETY: retained construction proves the iovec graph's exact byte sum and
// stable backing for 'a; cursor transitions emit each capped prefix once.
unsafe impl<'a: 'b, 'b> buffer::raw::Scatter<'b> for Cursor<'a> {
    fn remaining(&self) -> usize {
        self.remaining.into_raw()
    }

    unsafe fn advance<const MAX: usize>(
        &mut self,
        cap: NonZero<usize>,
        _left: NonZero<usize>,
    ) -> buffer::raw::Chunk<'b, MAX> {
        loop {
            // SAFETY: `left` is the exact positive remaining length and the
            // retained graph's byte sum proves a backing iovec exists.
            let iovec = unsafe { self.iovecs.get_unchecked(self.index as usize) };
            if iovec.len() == self.local as usize {
                self.index += 1;
                self.local = 0;
                continue;
            }
            // SAFETY: the non-empty retained iovec has live backing bytes;
            // cursor construction and transitions keep `local` within it.
            let bytes = unsafe { iovec.slice().get_unchecked(self.local as usize..) };
            let len = bytes.len().min(cap.get());
            self.local += len as u32;
            let consumed = transfer::Len::prefix(len, self.remaining);
            self.remaining = self.remaining.abs_diff(consumed);
            if self.local as usize == iovec.len() {
                self.index += 1;
                self.local = 0;
            }
            // SAFETY: retained graph construction proves exact length and
            // stable bytes; the cursor emits one non-empty capped prefix.
            return unsafe { buffer::raw::Chunk::new(&bytes[..len]) };
        }
    }
}

// SAFETY: Vectored retains its complete descriptor graph for 'a, and each cursor
// derives its position and remaining length from that same proved graph.
unsafe impl<'a, const N: usize> buffer::raw::Chunks for super::Vectored<'a, N> {
    type Cursor<'data>
        = Cursor<'a>
    where
        Self: 'data;

    fn len(&self) -> usize {
        self.bytes().into_raw()
    }

    fn start(&self) -> Self::Cursor<'_> {
        Cursor::start(self)
    }

    fn cursor(&self, offset: usize) -> Option<Self::Cursor<'_>> {
        Cursor::new(self, offset)
    }
}

const _: () = {
    let state = mem::size_of::<&[msg::Iovec]>() + 3 * mem::size_of::<u32>();
    let align = mem::align_of::<Cursor<'_>>();
    let exact = state.div_ceil(align) * align;
    assert!(mem::size_of::<Cursor<'_>>() == exact);
};
