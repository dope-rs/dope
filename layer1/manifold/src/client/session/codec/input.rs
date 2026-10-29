use std::{num, ops};

use consume::scope::{Brand, End, Error};
use dope_core::io::transfer;
use o3::buffer::{self, hold::scope, resident, storage::range, write::consume};

use crate::client::session::{
    codec::{Exact, Need, Parse, Progress, Step, staging},
    response,
};

pub struct Input<'id, 'a, 'd, R: buffer::Cursor> {
    inner: scope::Input<'a, 'd, R, Brand<'id>>,
    limit: staging::Limit,
}

impl<'id, 'a, 'd, R: buffer::Cursor> Input<'id, 'a, 'd, R> {
    /// Commits the exact input-scoped end before returning a decoded result.
    pub fn commit<T>(
        source: &mut R,
        budget: &resident::Budget<'d>,
        limit: staging::Limit,
        run: impl for<'scope, 'input> FnOnce(
            Input<'scope, 'input, 'd, R>,
        ) -> Result<Parse<'scope, T>, T>,
    ) -> Step<T>
    where
        R: buffer::raw::Consume,
        T: response::Outcome,
    {
        Self::consume(source, budget, limit, |input| match run(input) {
            Ok(Parse::More(need)) => {
                let Need { consumed, target } = need;
                let advanced = consumed.is_some();
                (consumed, Step(Progress::More { advanced, target }))
            }
            Ok(Parse::Blocked) => (None, Step::blocked()),
            Ok(Parse::CapacityExhausted) => (None, Step(Progress::CapacityExhausted)),
            Ok(Parse::Item { head, consumed }) => match head.disposition() {
                response::Direct::RetryInput => (None, Step(Progress::RetryInput)),
                response::Direct::Committed => (Some(consumed), Step(Progress::Item(head))),
            },
            Err(value) => (None, Step::rejected(value)),
        })
    }

    /// Requests growth while the parser continues to own the unchanged source.
    pub fn wait<T>(
        source: &'a R,
        budget: &'a resident::Budget<'d>,
        limit: staging::Limit,
        target: Option<usize>,
    ) -> Step<T> {
        Self::with(source, budget, limit, |input| {
            let need = match target {
                Some(target) => input.need_to(target),
                None => input.need_more(None),
            };
            match need {
                Some(need) => {
                    let Need { target, .. } = need;
                    Step(Progress::More {
                        advanced: false,
                        target,
                    })
                }
                None => Step(Progress::CapacityExhausted),
            }
        })
    }

    pub(in crate::client::session) fn consume<T>(
        source: &mut R,
        budget: &resident::Budget<'d>,
        limit: staging::Limit,
        run: impl for<'scope, 'input> FnOnce(
            Input<'scope, 'input, 'd, R>,
        )
            -> (Option<End<'scope, { transfer::MAX_BYTES as u32 }>>, T),
    ) -> T
    where
        R: buffer::raw::Consume,
    {
        scope::Input::consume(source, budget, |inner| run(Input { inner, limit }))
    }

    pub fn with<T>(
        source: &'a R,
        budget: &'a resident::Budget<'d>,
        limit: staging::Limit,
        run: impl for<'scope> FnOnce(Input<'scope, 'a, 'd, R>) -> T,
    ) -> T {
        scope::Input::new(source, budget).scope(|inner| run(Input { inner, limit }))
    }

    pub fn as_parts(&self) -> (&'a R, &'a resident::Budget<'d>) {
        self.inner.as_parts()
    }

    pub fn try_prefix(
        &self,
        count: num::NonZero<usize>,
    ) -> Result<
        (
            scope::Deferred<'a, 'd, R>,
            End<'id, { transfer::MAX_BYTES as u32 }>,
        ),
        Error,
    > {
        self.inner
            .try_prefix::<{ transfer::MAX_BYTES as u32 }>(count)
    }

    pub fn try_plan(
        &self,
        range: ops::Range<usize>,
    ) -> Result<scope::Deferred<'a, 'd, R>, range::Error> {
        self.inner.try_plan(range)
    }

    pub fn as_slice(&self) -> &'a [u8] {
        self.inner.as_slice()
    }

    pub fn try_consumed(
        &self,
        amount: num::NonZero<usize>,
    ) -> Result<End<'id, { transfer::MAX_BYTES as u32 }>, Error> {
        let (_, consumed) = self.try_prefix(amount)?;
        Ok(consumed)
    }

    pub fn need_more(
        &self,
        consumed: Option<End<'id, { transfer::MAX_BYTES as u32 }>>,
    ) -> Option<Need<'id>> {
        let left = self.len()
            - consumed
                .as_ref()
                .map_or(0, |used| used.count().get() as usize);
        match self.limit.0 {
            staging::Bound::Buffered(limit) if left >= limit.get() as usize => return None,
            staging::Bound::Direct if left != 0 => return None,
            staging::Bound::Buffered(_) | staging::Bound::Direct => {}
        }
        Some(Need {
            consumed,
            target: None,
        })
    }

    pub fn need_to(&self, target: usize) -> Option<Need<'id>> {
        let left = self.len();
        let limit = match self.limit.0 {
            staging::Bound::Direct => return None,
            staging::Bound::Buffered(limit) => limit.get() as usize,
        };
        if target <= left || target > limit {
            return None;
        }
        Some(Need {
            consumed: None,
            target: Some(Exact::from_valid(target)),
        })
    }
}

impl<'d, R: buffer::Cursor> ops::Deref for Input<'_, '_, 'd, R> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.inner.as_slice()
    }
}

impl<'d, R: buffer::Cursor> AsRef<[u8]> for Input<'_, '_, 'd, R> {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_slice()
    }
}
