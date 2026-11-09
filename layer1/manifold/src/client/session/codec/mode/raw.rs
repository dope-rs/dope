use std::{mem, num::NonZero, ops};

use o3::buffer::{
    self, hold,
    resident::{self, staging::Buf},
    storage::{self, range},
    write::{Sink as _, consume},
};

use crate::{
    app::receive::Failure,
    client::session::codec::{self, staging::Buffer},
};

pub(in crate::client::session) struct Source<'d, B: Buffer<'d>, C> {
    pub(super) stage: B,
    pub(super) received: Received<'d, B::Staged, C>,
}

pub(super) enum Received<'d, P, C> {
    /// An empty source has no nonzero prefix to consume.
    Vacant(&'d [u8; 0]),
    Staged {
        proof: P,
        pending: Option<C>,
        target: Option<codec::Exact>,
    },
    Chunk {
        chunk: C,
        active: bool,
    },
}

pub(in crate::client::session) enum View<B, C> {
    Vacant,
    Staged(B),
    Direct(C),
}

impl<'d, B: Buffer<'d>, C: buffer::Cursor + buffer::raw::Consume> Source<'d, B, C> {
    pub(super) fn new(stage: B) -> Self {
        Self {
            stage,
            received: Received::Vacant(&[]),
        }
    }

    /// Releases a fully consumed direct owner while preserving the empty stage.
    pub(super) fn direct(&mut self) {
        if let Received::Chunk { chunk, .. } = &self.received
            && buffer::Source::remaining(chunk) == 0
        {
            self.received = Received::Vacant(&[]);
        }
    }

    pub(super) fn prepare(&mut self, budget: &resident::Budget<'d>) -> Result<bool, Failure> {
        match &mut self.received {
            Received::Chunk { active, .. } => *active = true,
            Received::Staged {
                proof,
                pending,
                target,
            } => {
                let stage = self.stage.staged_mut(proof);
                if stage.as_ref().is_empty()
                    && let Some(chunk) = pending.take()
                {
                    self.received = Received::Chunk {
                        chunk,
                        active: true,
                    };
                } else if let Some(chunk) = pending {
                    let limit = match target {
                        Some(target) => target.count().get() as usize,
                        None => usize::MAX,
                    };
                    while let Some(count) = NonZero::new(
                        stage
                            .spare()
                            .min(limit - stage.len())
                            .min(chunk.as_ref().len()),
                    ) {
                        codec::Input::consume(chunk, budget, stage.limit(), |input| {
                            let (prefix, end) = match input.try_prefix(0, count) {
                                Ok(value) => value,
                                Err(error) => return (None, Err(Failure::Input(error))),
                            };
                            match stage.write_slice(prefix.as_slice()) {
                                Ok(()) => (Some(end), Ok(())),
                                Err(error) => (None, Err(Failure::Staging(error))),
                            }
                        })?;
                    }
                    if buffer::Source::remaining(chunk) == 0 {
                        *pending = None;
                    }
                }
            }
            Received::Vacant(_) => {}
        }
        self.direct();
        Ok(buffer::Source::remaining(self) != 0)
    }

    pub(super) fn preserve(
        &mut self,
        target: Option<codec::Exact>,
        budget: &resident::Budget<'d>,
    ) -> Result<bool, Failure> {
        let len = self.as_ref().len();
        if len != 0 || target.is_some() {
            let proof = self
                .stage
                .try_reserve(target.map(|exact| {
                    NonZero::<usize>::MIN.saturating_add(exact.count().get() as usize - 1)
                }))
                .map_err(Failure::Staging)?;
            let result = if let Received::Chunk { chunk, .. } = &mut self.received {
                let stage = self.stage.staged_mut(&proof);
                stage
                    .write_slice(chunk.as_ref())
                    .map_err(Failure::Staging)?;
                match NonZero::new(len) {
                    None => Ok(()),
                    Some(count) => codec::Input::consume(chunk, budget, stage.limit(), |input| {
                        match input.try_consumed(count) {
                            Ok(end) => (Some(end), Ok(())),
                            Err(error) => (None, Err(Failure::Input(error))),
                        }
                    }),
                }
            } else {
                Ok(())
            };
            self.received = Received::Staged {
                proof,
                pending: match mem::replace(&mut self.received, Received::Vacant(&[])) {
                    Received::Chunk { chunk, .. } => {
                        (buffer::Source::remaining(&chunk) != 0).then_some(chunk)
                    }
                    Received::Staged { pending, .. } => pending,
                    Received::Vacant(_) => None,
                },
                target,
            };
            result?;
        }
        Ok(matches!(
            &self.received,
            Received::Staged {
                pending: Some(_),
                ..
            }
        ))
    }
}

impl<'d, B: Buffer<'d>, C: buffer::Source> buffer::Source for Source<'d, B, C> {
    type View<'input>
        = View<B::View<'input>, C::View<'input>>
    where
        Self: 'input;

    fn remaining(&self) -> usize {
        match &self.received {
            Received::Vacant(bytes) => bytes.len(),
            Received::Staged { proof, .. } => self.stage.staged(proof).remaining(),
            Received::Chunk { chunk, .. } => chunk.remaining(),
        }
    }
}

impl<'d, B: Buffer<'d>, C: AsRef<[u8]>> AsRef<[u8]> for Source<'d, B, C> {
    fn as_ref(&self) -> &[u8] {
        match &self.received {
            Received::Vacant(_) => &[],
            Received::Staged { proof, .. } => self.stage.staged(proof).as_ref(),
            Received::Chunk { chunk, .. } => chunk.as_ref(),
        }
    }
}

impl<'d, B: Buffer<'d>, C: buffer::Cursor> buffer::Cursor for Source<'d, B, C> {
    fn try_get<'input>(
        &'input self,
        range: ops::Range<usize>,
    ) -> Result<Self::View<'input>, range::Error> {
        match &self.received {
            Received::Vacant(bytes) => {
                <[u8] as buffer::Cursor>::try_get(bytes.as_slice(), range).map(|_| View::Vacant)
            }
            Received::Staged { proof, .. } => self.stage.try_view(proof, range).map(View::Staged),
            Received::Chunk { chunk, .. } => chunk.try_get(range).map(View::Direct),
        }
    }
}

impl<'input, B: buffer::Read<'input>, C: buffer::Read<'input>> buffer::Read<'input> for View<B, C> {
    fn as_slice(&self) -> &'input [u8] {
        match self {
            Self::Vacant => &[],
            Self::Staged(view) => view.as_slice(),
            Self::Direct(view) => view.as_slice(),
        }
    }

    fn try_split<const N: usize>(self) -> Result<(&'input [u8; N], Self), consume::Error> {
        Ok(match self {
            Self::Vacant => {
                let (head, _) = <&[u8] as buffer::Read<'input>>::try_split::<N>(&[])?;
                (head, Self::Vacant)
            }
            Self::Staged(view) => {
                let (head, tail) = view.try_split::<N>()?;
                (head, Self::Staged(tail))
            }
            Self::Direct(view) => {
                let (head, tail) = view.try_split::<N>()?;
                (head, Self::Direct(tail))
            }
        })
    }
}

const _: () = {
    use std::mem::size_of;

    use dope_net::wire::{self, receive::Chunk};

    use crate::client::session::codec::staging::Empty;

    type Direct = Source<'static, Empty<'static>, Chunk<'static, 'static, wire::Identity>>;
    type Buffered = Source<'static, Buf<'static>, Chunk<'static, 'static, wire::Identity>>;
    assert!(size_of::<Direct>() == 4 * size_of::<usize>());
    assert!(size_of::<Buffered>() == 9 * size_of::<usize>());
    assert!(size_of::<<Direct as buffer::Source>::View<'static>>() == 3 * size_of::<usize>());
    assert!(size_of::<<Buffered as buffer::Source>::View<'static>>() == 4 * size_of::<usize>());
};

// SAFETY: empty views expose no storage; other variants delegate selection,
// splitting, and retention to their original source.
unsafe impl<'d, B: Buffer<'d>, C: hold::raw::Retain<'d>> hold::raw::Retain<'d>
    for Source<'d, B, C>
{
    fn resident<'a>(plan: &Self::View<'a>) -> usize
    where
        Self: 'a,
    {
        match plan {
            View::Vacant => 0,
            View::Staged(view) => Buf::resident(B::staged_view(view)),
            View::Direct(view) => C::resident(view),
        }
    }

    fn try_copy<'a>(
        plan: &Self::View<'a>,
        budget: &resident::Budget<'d>,
    ) -> Result<storage::Owned<resident::Budget<'d>>, hold::Error>
    where
        Self: 'a,
    {
        match plan {
            View::Vacant => Ok(storage::Owned::default()),
            View::Staged(view) => Buf::try_copy(B::staged_view(view), budget),
            View::Direct(view) => C::try_copy(view, budget),
        }
    }

    fn try_retain<'a>(
        plan: &Self::View<'a>,
        budget: &resident::Budget<'d>,
    ) -> Result<hold::Retained<'d>, hold::Error>
    where
        Self: 'a,
    {
        match plan {
            View::Vacant => Ok(resident::Shared::new().into()),
            View::Staged(view) => Buf::try_retain(B::staged_view(view), budget),
            View::Direct(view) => C::try_retain(view, budget),
        }
    }
}

// SAFETY: every view and consumption selects the same exclusively owned source.
unsafe impl<'d, B: Buffer<'d>, C: buffer::raw::Consume> buffer::raw::Consume for Source<'d, B, C> {
    fn consume<const MAX: u32>(prefix: buffer::raw::Consumed<'_, '_, Self, MAX>) {
        let (source, end) = prefix.into_parts();
        match &mut source.received {
            Received::Vacant(_) => {}
            Received::Staged { proof, target, .. } => {
                // SAFETY: the staged variant exposes exactly the unchanged stage range.
                Buf::consume(unsafe {
                    buffer::raw::Consumed::from_parts(source.stage.staged_mut(proof), end)
                });
                *target = None;
            }
            Received::Chunk { chunk, .. } => {
                // SAFETY: the direct variant exposes exactly this unchanged chunk range.
                C::consume(unsafe { buffer::raw::Consumed::from_parts(chunk, end) });
            }
        }
    }
}
