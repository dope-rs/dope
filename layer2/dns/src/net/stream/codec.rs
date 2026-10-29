use std::num;

use dope::manifold::client::session::codec::{
    Codec, Finish, Input, Parse, Stateless, eof, staging,
};
use o3::buffer::{self, hold::scope::Deferred, resident::staging::Plan, write::consume::scope};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Input(scope::Error),
    Truncated { actual: usize, required: usize },
}

/// Network-order unsigned 16-bit DNS stream framing.
pub struct Prefix;

impl Codec for Prefix {
    type Head<'input, 'd, R: buffer::Cursor>
        = Deferred<'input, 'd, R>
    where
        R: 'input,
        'd: 'input;
    type Error = Error;
    type Staging = staging::Buffered;

    fn staging(&self) -> <Self::Staging as staging::Policy>::Spec {
        const WARM: usize = 16 * 1024;
        let limit = size_of::<u16>() + usize::from(u16::MAX);
        Plan::new(limit.min(WARM), limit)
    }
}

impl Stateless for Prefix {
    fn parse<'id, 'input, 'd, R: buffer::Cursor>(
        &self,
        buf: Input<'id, 'input, 'd, R>,
    ) -> Result<Parse<'id, Self::Head<'input, 'd, R>>, Self::Error>
    where
        'd: 'input,
    {
        const PREFIX: usize = size_of::<u16>();
        let bytes = buf.as_slice();
        if bytes.len() < PREFIX {
            return Ok(match buf.need_to(PREFIX) {
                Some(need) => Parse::More(need),
                None => Parse::CapacityExhausted,
            });
        }
        let length = usize::from(u16::from_be_bytes([bytes[0], bytes[1]]));
        let total = num::NonZero::<usize>::MIN.saturating_add(PREFIX - 1 + length);
        let payload = match buf.try_plan(PREFIX..total.get()) {
            Ok(payload) => payload,
            Err(error) => {
                return Ok(match buf.need_to(error.end()) {
                    Some(need) => Parse::More(need),
                    None => Parse::CapacityExhausted,
                });
            }
        };
        let consumed = buf.try_consumed(total).map_err(Error::Input)?;
        Ok(Parse::Item {
            head: payload,
            consumed,
        })
    }
}

impl eof::Finish for Prefix {
    fn finish<'id, 'input, 'd, R: buffer::Cursor>(
        &self,
        input: Input<'id, 'input, 'd, R>,
    ) -> Result<Finish<Self::Head<'input, 'd, R>>, Self::Error>
    where
        'd: 'input,
    {
        const PREFIX: usize = size_of::<u16>();
        let bytes = input.as_slice();
        let left = bytes.len();
        if left == 0 {
            return Ok(Finish::Empty);
        }
        let required = if left < PREFIX {
            PREFIX
        } else {
            let length = usize::from(u16::from_be_bytes([bytes[0], bytes[1]]));
            PREFIX + length
        };
        Err(Error::Truncated {
            actual: left,
            required,
        })
    }
}
