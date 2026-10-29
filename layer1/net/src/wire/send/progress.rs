use std::marker;

use dope_core::io::{socket::msg, transfer, transfer::progress};
use o3::buffer::segments::scatter;

use crate::wire::send;

/// One send-storage transaction bound to its exact input cursor.
#[must_use = "input progress must be finished into the bound send storage"]
pub struct Plan<'a, S: send::Store, I> {
    send: send::Storage<'a, S>,
    input: I,
}

/// Exclusive progress access which cannot replace the admitted input cursor.
#[repr(transparent)]
pub struct Input<'a, I>(&'a mut I);

/// Cursor over one retained policy input with at most `N` descriptors.
pub struct Vectored<'data, const N: usize = { msg::MAX_IOVECS }> {
    chunks: msg::raw::Cursor<'data>,
    total: transfer::Len,
    input: marker::PhantomData<fn(&'data ()) -> &'data ()>,
}

/// Tentative bounded traversal; dropping it preserves the original cursor.
#[must_use = "a successful traversal must be committed"]
pub struct Parts<'a, 'data, const N: usize = { msg::MAX_IOVECS }> {
    owner: &'a mut Vectored<'data, N>,
    trial: msg::raw::Cursor<'data>,
    end: transfer::Len,
}

impl<'a, S: send::Store, I> Plan<'a, S, I> {
    pub fn parts(&mut self) -> (&mut S, Input<'_, I>) {
        (&mut self.send, Input(&mut self.input))
    }

    pub fn into_storage(self) -> send::Storage<'a, S> {
        self.send
    }
}

impl<'a, S: send::Store> Plan<'a, S, progress::Bytes<'a>> {
    pub fn plain(send: send::Storage<'a, S>, plain: send::Plain<'a>) -> Self {
        Self {
            send,
            input: progress::Bytes::new(plain.into_view()),
        }
    }

    pub fn finish(self) -> send::Consumed<'a, S> {
        send::Consumed {
            storage: self.send,
            consumed: self.input.into_len(),
        }
    }
}

impl<'data> Input<'_, progress::Bytes<'data>> {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn take(&mut self, limit: transfer::Len) -> progress::Slice<'_, 'data> {
        self.0.take(limit)
    }
}

impl<'data, const N: usize> Input<'_, Vectored<'data, N>> {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn remaining(&self) -> transfer::Len {
        self.0.remaining()
    }

    pub fn take(&mut self, limit: transfer::Len) -> Parts<'_, 'data, N> {
        self.0.take(limit)
    }
}

impl<'data, const N: usize> Vectored<'data, N> {
    fn new(msg: &send::Vectored<'data, N>) -> Self {
        Self {
            chunks: msg.chunks(),
            total: msg.bytes(),
            input: marker::PhantomData,
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.chunks.bytes().is_zero()
    }

    #[must_use]
    pub fn remaining(&self) -> transfer::Len {
        self.chunks.bytes()
    }

    pub fn take(&mut self, limit: transfer::Len) -> Parts<'_, 'data, N> {
        let trial = self.chunks;
        let end = self.remaining().abs_diff(limit.min(self.remaining()));
        Parts {
            owner: self,
            trial,
            end,
        }
    }
}

impl<'data, S: send::Store, const N: usize> Plan<'data, S, Vectored<'data, N>> {
    pub fn vectored(send: send::Storage<'data, S>, plain: &send::Vectored<'data, N>) -> Self {
        Self {
            send,
            input: Vectored::new(plain),
        }
    }

    pub fn finish(self) -> send::Consumed<'data, S> {
        send::Consumed {
            storage: self.send,
            consumed: self.input.total.abs_diff(self.input.remaining()),
        }
    }
}

impl<'a, 'data, const N: usize> Parts<'a, 'data, N> {
    #[must_use]
    pub fn amount(&self) -> transfer::Len {
        self.owner.remaining().abs_diff(self.end)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.trial.bytes() == self.end
    }

    /// Traverses a tentative prefix without advancing the retained input.
    pub fn take(&mut self) -> scatter::Take<'_, 'data, msg::raw::Cursor<'data>> {
        let left = self.trial.bytes().abs_diff(self.end);
        scatter::Take::new(&mut self.trial, left.into_raw())
    }

    /// Commits only the prefix actually traversed under this exclusive owner.
    pub fn commit(self) -> transfer::Len {
        let used = self.owner.remaining().abs_diff(self.trial.bytes());
        self.owner.chunks = self.trial;
        used
    }
}

const _: () = {
    assert!(
        std::mem::size_of::<Input<'static, progress::Bytes<'static>>>()
            == std::mem::size_of::<&mut progress::Bytes<'static>>()
    );
    assert!(std::mem::size_of::<Parts<'static, 'static>>() <= 6 * std::mem::size_of::<usize>());
    assert!(
        std::mem::size_of::<Vectored<'_>>()
            == std::mem::size_of::<msg::raw::Cursor<'_>>() + std::mem::size_of::<usize>()
    );
    assert!(
        std::mem::size_of::<Plan<'static, (), progress::Bytes<'static>>>()
            == std::mem::size_of::<send::Storage<'static, ()>>()
                + std::mem::size_of::<progress::Bytes<'static>>()
    );
    assert!(
        std::mem::align_of::<Plan<'static, (), progress::Bytes<'static>>>()
            == std::mem::align_of::<send::Storage<'static, ()>>()
    );
};
