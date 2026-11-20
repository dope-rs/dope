use crate::state::{direct, staged};
use o3::base::num::bounded;
use o3::buffer::pool::owners;
use std::num;

use crate::tls::receive::stream::{Backing, Retained, Staged};
use dope::core::driver::route::Identity;
use dope::core::io::{recv, transfer};
use dope::net::wire;
use dope::net::wire::receive::{self, batch};
use o3::buffer::write::consume::{self, Consume};
use o3::buffer::{self, bytes, hold, pool, resident, storage};

impl<'w, 'd> Retained<'w, 'd> {
    pub(in crate::tls::receive) fn try_new(
        staged: Option<Staged<'w>>,
        plain: direct::source::Cursor<'d>,
    ) -> Result<Option<Self>, error::Error> {
        let staged = staged.filter(|chunk| !buffer::Source::is_empty(chunk));
        let body = plain.remaining();
        let plain = (body != 0).then_some(plain);
        let stage = staged.as_ref().map_or(0, buffer::Source::remaining);
        let len = bounded::Usize::<{ transfer::MAX_BYTES }>::ZERO
            .try_add(stage)
            .map_err(error::Error::Staged)?
            .try_add(body)
            .map_err(error::Error::Retained)?;
        if len.get() == 0 {
            return Ok(None);
        }
        let staged = staged.map(|chunk| {
            // SAFETY: the first addition admitted this stable owner's exact length within MAX_BYTES.
            bytes::bounded::Value::from_raw(unsafe { bytes::bounded::raw::Input::new(chunk) })
        });
        Ok(Some(Self {
            staged,
            plain,
            remaining: transfer::Len::from_count(len),
        }))
    }

    // `used` must be the exact byte count just consumed from this cursor's head.
    unsafe fn advance(&mut self, used: usize) {
        let left = self.remaining.into_raw() - used;
        // SAFETY: the consumed head prefix fit the admitted total, so left also fits MAX_BYTES.
        self.remaining = transfer::Len::from(unsafe { bounded::raw::u32(left as u32) });
        if self.staged.as_ref().is_some_and(buffer::Source::is_empty) {
            self.staged = None;
        }
    }
}

impl Consume for Retained<'_, '_> {
    fn remaining(&self) -> usize {
        self.as_ref().len()
    }

    fn try_consume(&mut self, amount: usize) -> Result<(), consume::Error> {
        if let Some(staged) = &mut self.staged {
            staged.try_consume(amount)?;
        } else if let Some(plain) = &mut self.plain {
            plain.try_consume(amount)?;
        } else {
            return consume::Error::check(amount, 0);
        }
        // SAFETY: the selected head successfully consumed exactly amount.
        unsafe { self.advance(amount) };
        Ok(())
    }

    fn consume_max(&mut self, max: usize) -> usize {
        let used = if let Some(staged) = &mut self.staged {
            staged.consume_max(max)
        } else {
            self.plain.as_mut().map_or(0, |plain| plain.consume_max(max))
        };
        // SAFETY: the selected head returned the count it consumed, or there was no head.
        unsafe { self.advance(used) };
        used
    }
}

// SAFETY: the admitted prefix, view and slice select the same exclusive head.
unsafe impl buffer::raw::Consume for Retained<'_, '_> {
    fn consume<const MAX: u32>(prefix: buffer::raw::Consumed<'_, '_, Self, MAX>) {
        let (cursor, end) = prefix.into_parts();
        let used = end.count().get() as usize;
        if let Some(staged) = &mut cursor.staged {
            // SAFETY: a present staged owner is the exact current head.
            buffer::raw::Consume::consume(unsafe { buffer::raw::Consumed::from_parts(staged, end) });
        } else if let Some(plain) = &mut cursor.plain {
            // SAFETY: the current view is this cursor's authenticated plaintext suffix.
            buffer::raw::Consume::consume(unsafe { buffer::raw::Consumed::from_parts(plain, end) });
        } else {
            unreachable!("head");
        }
        // SAFETY: the exclusive head consumed the prefix identified by this same end.
        unsafe { cursor.advance(used) };
    }
}

use crate::{
    error,
    tls::{
        self,
        receive::{Batch, Ingress},
        roles,
    },
};

pub(super) struct Producer<'s, 'd, 'input, 'f, R: roles::Protocol + 'd, ID: Identity + 'd> {
    pub(super) ingress: Ingress<'s, 'd, R, ID>,
    pub(super) bytes: &'input mut [u8],
    pub(super) control: &'f mut (Result<(), error::Error>, bool),
}

// SAFETY: Input::owned returns one pooled chunk and one initialized input prefix at most.
// Both retain their actual storage owner, and the capacity admits two items.
unsafe impl<'d: 'input, 'input, R: roles::Protocol + 'd, ID: Identity + 'd>
    receive::raw::Producer<'input, tls::Tls<R>> for Producer<'_, 'd, 'input, '_, R, ID>
{
    fn produce(self, _cap: &batch::Capacity<'_, tls::Tls<R>>) -> Batch<'input> {
        let (
            staged::Output {
                chunk,
                control: failure,
            },
            plain,
        ) = direct::Input::owned(self.ingress.conn, self.bytes, self.ingress.recv);
        *self.control = (failure, chunk.is_none());
        Batch {
            owned: chunk,
            plain: plain.into_chunk(),
        }
    }
}

const _: () = {
    use wire::receive::raw;

    // SAFETY: next takes each of the two optional owners once; len counts those
    // same owners, and every yielded slice retains its authenticated lifetime.
    unsafe impl<'a> raw::Batch<'a> for Batch<'a> {
        type Item =
            bytes::Source<'a, storage::Shared<owners::Edge<owners::Plain, pool::Borrowed<'a>>>>;

        const MAX_ITEMS: num::NonZero<usize> = num::NonZero::<usize>::MIN.saturating_add(1);

        fn len(&self) -> usize {
            usize::from(self.owned.is_some()) + usize::from(self.plain.is_some())
        }

        fn next(&mut self) -> Option<Self::Item> {
            if let Some(owned) = self.owned.take() {
                return Some(bytes::Source::Owned(owned));
            }
            let plain = self.plain.take()?;
            Some(bytes::Source::Borrowed(plain))
        }
    }
};

// SAFETY: Each immutable backing view retains its exact range or copies those exact bytes.
unsafe impl<'w, 'd> hold::raw::Retain<'d> for Retained<'w, 'd> {
    fn try_copy<'a>(
        plan: &Self::View<'a>,
        budget: &resident::Budget<'d>,
    ) -> Result<storage::Owned<resident::Budget<'d>>, hold::Error>
    where
        Self: 'a,
    {
        match &plan.backing {
            Backing::Empty => Ok(storage::Owned::default()),
            Backing::Staged(plan) => {
                <storage::Shared<_> as hold::raw::Retain<'d>>::try_copy(plan, budget)
            }
            Backing::Plain(plan) => {
                <recv::Shared<'d> as hold::raw::Retain<'d>>::try_copy(plan, budget)
            }
        }
    }

    fn resident<'a>(plan: &Self::View<'a>) -> usize
    where
        Self: 'a,
    {
        match &plan.backing {
            Backing::Empty => 0,
            Backing::Staged(plan) => <storage::Shared<
                owners::Edge<owners::Plain, pool::Borrowed<'w>>,
            > as hold::raw::Retain<'w>>::resident(plan),
            Backing::Plain(plan) => <recv::Shared<'d> as hold::raw::Retain<'d>>::resident(plan),
        }
    }

    fn try_retain<'a>(
        plan: &Self::View<'a>,
        budget: &resident::Budget<'d>,
    ) -> Result<hold::Retained<'d>, hold::Error>
    where
        Self: 'a,
    {
        match &plan.backing {
            Backing::Empty => Ok(resident::Shared::new().into()),
            Backing::Staged(plan) => {
                <storage::Shared<_> as hold::raw::Retain<'d>>::try_retain(plan, budget)
            }
            Backing::Plain(plan) => {
                <recv::Shared<'d> as hold::raw::Retain<'d>>::try_retain(plan, budget)
            }
        }
    }
}
