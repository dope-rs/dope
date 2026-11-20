use std::marker::PhantomData;

use dope_core::io::transfer;
use o3::base::marker::ThreadBound;
use o3::buffer::{
    hold, resident,
    write::consume::{self, scope::Error},
};

use crate::wire::{
    self,
    receive::{
        Chunk,
        mode::{Disabled, access::Cursor as _},
    },
};

/// A nonempty remainder of this callback's original cursor.
pub struct Pending<'s>(PhantomData<&'s mut &'s ThreadBound>);

/// The exact remaining input and its resident budget.
pub struct Input<'s, 'w, 'd: 'w, W: wire::Spec + 'w> {
    current: Option<(&'s mut Chunk<'w, 'd, W, Disabled>, transfer::NonZero)>,
    budget: &'s resident::Budget<'d>,
    pending: Pending<'s>,
}

/// An exclusive loan of one admitted, contiguous head.
pub struct Head<'s, 'w, 'd: 'w, W: wire::Spec + 'w> {
    chunk: &'s mut Chunk<'w, 'd, W, Disabled>,
    len: transfer::NonZero,
    budget: &'s resident::Budget<'d>,
    pending: Pending<'s>,
}

/// On `Some` the owner releases the cursor. On `None` it parks the exact remaining suffix.
pub fn with<'w, 'd: 'w, W: wire::Spec + 'w, T>(
    chunk: &mut Chunk<'w, 'd, W, Disabled>,
    budget: &resident::Budget<'d>,
    run: impl for<'s> FnOnce(Input<'s, 'w, 'd, W>) -> Result<T, Pending<'s>>,
) -> Result<Option<T>, Error> {
    let current = chunk.inner.0.head_len()?.map(|len| (chunk, len));
    Ok(run(Input {
        current,
        budget,
        pending: Pending(PhantomData),
    })
    .ok())
}

impl<'s, 'w, 'd: 'w, W: wire::Spec + 'w> Input<'s, 'w, 'd, W> {
    pub const fn budget(&self) -> &'s resident::Budget<'d> {
        self.budget
    }

    pub fn head(self) -> Option<Head<'s, 'w, 'd, W>> {
        let (chunk, len) = self.current?;
        Some(Head {
            chunk,
            len,
            budget: self.budget,
            pending: self.pending,
        })
    }

    pub fn settle(self) -> Result<(), Pending<'s>> {
        match self.current {
            Some(_) => Err(self.pending),
            None => Ok(()),
        }
    }
}

impl<'s, 'w, 'd: 'w, W: wire::Spec + 'w> Head<'s, 'w, 'd, W> {
    pub const fn len(&self) -> transfer::NonZero {
        self.len
    }

    pub fn input(&self) -> hold::scope::Input<'_, 'd, Chunk<'w, 'd, W, Disabled>> {
        hold::scope::Input::new(&*self.chunk, self.budget)
    }

    pub fn defer(self) -> Pending<'s> {
        self.pending
    }

    pub fn finish(self) -> Result<Input<'s, 'w, 'd, W>, Error> {
        let next = self.chunk.inner.0.successor()?;
        Ok(self.rest(next))
    }

    pub fn consume_max(self, count: usize) -> Result<Input<'s, 'w, 'd, W>, Error> {
        let len = transfer::Len::from_nonzero(self.len);
        let Some(left) = len.checked_sub(transfer::Len::prefix(count, len)) else {
            return self.finish();
        };
        consume::Consume::consume_max(self.chunk, count);
        Ok(self.rest(Some(left)))
    }

    fn rest(self, len: Option<transfer::NonZero>) -> Input<'s, 'w, 'd, W> {
        Input {
            current: len.map(|len| (self.chunk, len)),
            budget: self.budget,
            pending: self.pending,
        }
    }
}
