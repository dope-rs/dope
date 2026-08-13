use std::{num, ops};

use dope_net::wire;
use o3::buffer::resident;

mod error;
mod frame;

pub use error::Error;
pub use frame::Frame;

/// Incremental parse result; only `NeedMore` retains ingress for a later read.
pub enum Parse<T> {
    NeedMore,
    CapacityExhausted,
    Item {
        head: T,
        consumed: num::NonZeroUsize,
    },
}

#[must_use]
pub enum Retain<'d> {
    Ready(wire::RetainedBytes<'d>),
    NeedMore,
    CapacityExhausted,
}

pub struct Input<'a, 'd, R: wire::Cursor<'d>> {
    source: &'a R,
    budget: &'a resident::Budget<'d>,
}

impl<'a, 'd, R: wire::Cursor<'d>> Input<'a, 'd, R> {
    pub fn new(source: &'a R, budget: &'a resident::Budget<'d>) -> Self {
        Self { source, budget }
    }

    pub fn retain(&self, range: ops::Range<usize>) -> Retain<'d> {
        match self.source.retain(range, self.budget) {
            Ok(bytes) => Retain::Ready(bytes),
            Err(wire::RetainError::Range) => Retain::NeedMore,
            Err(wire::RetainError::Capacity) => Retain::CapacityExhausted,
        }
    }

    pub fn retain_all(&self) -> Retain<'d> {
        let len = self.source.chunk().len();
        self.retain(0..len)
    }

    pub fn as_slice(&self) -> &'a [u8] {
        self.source.chunk()
    }
}

impl<'d, R: wire::Cursor<'d>> ops::Deref for Input<'_, 'd, R> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<'d, R: wire::Cursor<'d>> AsRef<[u8]> for Input<'_, 'd, R> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

pub trait Codec {
    /// A decoded item borrowing `input`; retained storage is branded by `d`.
    /// Bytes kept beyond a response callback must be retained into `d`.
    type Head<'input, 'd>;
    type ParseState;
    type Error;

    fn parse_state(&self) -> Self::ParseState;

    fn parse<'input, 'd, R: wire::Cursor<'d>>(
        &self,
        state: &mut Self::ParseState,
        buf: Input<'input, 'd, R>,
    ) -> Result<Parse<Self::Head<'input, 'd>>, Self::Error>
    where
        'd: 'input;

    /// Finalizes a parser after EOF, rejecting invalid or truncated ingress.
    fn finish<'d>(
        &self,
        state: &mut Self::ParseState,
        remaining: wire::RetainedBytes<'d>,
    ) -> Result<Option<Self::Head<'d, 'd>>, Self::Error>;
}

/// Network-order unsigned 16-bit length framing with an explicit payload ceiling.
pub struct LengthPrefixed<const MAX: usize>;

impl<const MAX: usize> Codec for LengthPrefixed<MAX> {
    type Head<'input, 'd> = Frame<'d>;
    type ParseState = ();
    type Error = Error;

    fn parse_state(&self) {}

    fn parse<'input, 'd, R: wire::Cursor<'d>>(
        &self,
        _state: &mut (),
        buf: Input<'input, 'd, R>,
    ) -> Result<Parse<Self::Head<'input, 'd>>, Self::Error>
    where
        'd: 'input,
    {
        const PREFIX: usize = size_of::<u16>();
        if buf.len() < PREFIX {
            return Ok(Parse::NeedMore);
        }
        let length = usize::from(u16::from_be_bytes([buf[0], buf[1]]));
        if length > MAX {
            return Err(Error::Capacity { length, limit: MAX });
        }
        let total = PREFIX + length;
        let payload = match buf.retain(PREFIX..total) {
            Retain::Ready(payload) => payload,
            Retain::NeedMore => return Ok(Parse::NeedMore),
            Retain::CapacityExhausted => return Ok(Parse::CapacityExhausted),
        };
        let consumed = num::NonZeroUsize::MIN.saturating_add(total - 1);
        Ok(Parse::Item {
            head: Frame(payload),
            consumed,
        })
    }

    fn finish<'d>(
        &self,
        _state: &mut (),
        remaining: wire::RetainedBytes<'d>,
    ) -> Result<Option<Self::Head<'d, 'd>>, Self::Error> {
        if remaining.is_empty() {
            Ok(None)
        } else {
            Err(Error::Truncated)
        }
    }
}
