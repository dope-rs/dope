use o3::buffer::Complete as _;
use std::{mem, ops};

use direct::source::Cursor;
use dope::{
    core::io::{recv, transfer},
    net::wire::receive::mode::access,
};
use o3::buffer::{
    self, bytes::bounded, pool,
    pool::owners,
    storage::{self, range},
    write::consume,
};

use crate::state::direct;

pub(super) type Staged<'w> = storage::Shared<owners::Edge<owners::Plain, pool::Borrowed<'w>>>;

/// Authenticated plaintext in its original pooled and provided owners.
pub struct Retained<'w, 'd> {
    pub(super) staged: Option<bounded::Value<Staged<'w>>>,
    pub(super) plain: Option<Cursor<'d>>,
    pub(super) remaining: transfer::Len,
}

impl<'w, 'd> buffer::Source for Retained<'w, 'd> {
    type View<'input>
        = Retention<'input, 'w, 'd>
    where
        Self: 'input;

    fn remaining(&self) -> usize {
        self.remaining.into_raw()
    }
}

impl transfer::Length for Retained<'_, '_> {
    fn len(&self) -> transfer::Len {
        self.remaining
    }
}

impl AsRef<[u8]> for Retained<'_, '_> {
    fn as_ref(&self) -> &[u8] {
        if let Some(staged) = &self.staged {
            staged.as_ref()
        } else {
            self.plain.as_ref().map_or(&[], AsRef::as_ref)
        }
    }
}

impl<'w, 'd> buffer::Complete for Retained<'w, 'd> {
    fn view(&self) -> Self::View<'_> {
        let backing = if let Some(staged) = &self.staged {
            Backing::Staged(staged.view().into_inner())
        } else if let Some(plain) = &self.plain {
            Backing::Plain(plain.view())
        } else {
            Backing::Empty
        };
        Retention { backing }
    }
}

impl<'w, 'd> buffer::Cursor for Retained<'w, 'd> {
    fn try_get<'input>(
        &'input self,
        range: ops::Range<usize>,
    ) -> Result<Self::View<'input>, range::Error> {
        let backing = match self.view().backing {
            Backing::Staged(view) => Backing::Staged(view.try_get(range)?),
            Backing::Plain(view) => Backing::Plain(view.try_get(range)?),
            Backing::Empty => {
                <[u8] as buffer::Cursor>::try_get(&[], range)?;
                Backing::Empty
            }
        };
        Ok(Retention { backing })
    }
}

impl<'d> access::Cursor<'d> for Retained<'_, 'd> {
    fn head_len(&self) -> Result<Option<transfer::NonZero>, consume::scope::Error> {
        Ok(self.staged.as_ref().map_or(self.remaining, transfer::Length::len).nonzero())
    }

    fn successor(&mut self) -> Result<Option<transfer::NonZero>, consume::scope::Error> {
        if self.staged.is_none() {
            return Ok(None);
        }
        self.staged = None;
        self.remaining = self.plain.as_ref().map_or(transfer::Len::ZERO, |plain| {
            transfer::Length::len(&plain.view())
        });
        Ok(self.remaining.nonzero())
    }
}

const _: () = assert!(
    mem::size_of::<Retained<'_, '_>>()
        <= mem::size_of::<Option<storage::Shared<owners::Edge<owners::Plain, pool::Borrowed<'_>>>>>(
        ) + mem::size_of::<Option<Cursor<'_>>>()
            + mem::size_of::<usize>()
);

/// Checked TLS plaintext tied to the backing proof without retention or accounting.
#[doc(hidden)]
pub struct Retention<'input, 'w: 'input, 'd: 'input> {
    pub(super) backing: Backing<'input, 'w, 'd>,
}

pub(super) enum Backing<'input, 'w: 'input, 'd: 'input> {
    Empty,
    Staged(
        <storage::Shared<owners::Edge<owners::Plain, pool::Borrowed<'w>>> as buffer::Source>::View<
            'input,
        >,
    ),
    Plain(<recv::Shared<'d> as buffer::Source>::View<'input>),
}

impl<'input, 'w: 'input, 'd: 'input> buffer::Read<'input> for Retention<'input, 'w, 'd> {
    fn as_slice(&self) -> &'input [u8] {
        match &self.backing {
            Backing::Empty => &[],
            Backing::Staged(plan) => buffer::Read::as_slice(plan),
            Backing::Plain(plan) => buffer::Read::as_slice(plan),
        }
    }

    fn try_split<const N: usize>(self) -> Result<(&'input [u8; N], Self), consume::Error> {
        let (head, backing) = match self.backing {
            Backing::Empty => {
                let (head, _) = buffer::Read::try_split(&[][..])?;
                (head, Backing::Empty)
            }
            Backing::Staged(plan) => {
                let (head, tail) = buffer::Read::try_split(plan)?;
                (head, Backing::Staged(tail))
            }
            Backing::Plain(plan) => {
                let (head, tail) = buffer::Read::try_split(plan)?;
                (head, Backing::Plain(tail))
            }
        };
        Ok((head, Self { backing }))
    }
}

const _: () = assert!(mem::size_of::<Retention<'_, '_, '_>>() == 4 * mem::size_of::<usize>());
