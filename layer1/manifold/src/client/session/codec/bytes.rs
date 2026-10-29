use std::{mem::size_of, num::NonZero};

use o3::{
    buffer,
    buffer::{hold::scope::Deferred, write::consume::scope},
};

use crate::client::session::codec::{
    Codec, End, Finish, Input, Parse, Stateless, eof, staging::Direct,
};

/// Defers each non-empty receive chunk without interpreting its bytes.
pub struct Bytes;

impl Codec for Bytes {
    type Head<'input, 'd, R: buffer::Cursor>
        = Deferred<'input, 'd, R>
    where
        R: 'input,
        'd: 'input;
    type Error = scope::Error;
    type Staging = Direct;

    fn staging(&self) -> Direct {
        Direct
    }
}

impl Stateless for Bytes {
    fn parse<'id, 'input, 'd, R: buffer::Cursor>(
        &self,
        input: Input<'id, 'input, 'd, R>,
    ) -> Result<Parse<'id, Self::Head<'input, 'd, R>>, Self::Error>
    where
        'd: 'input,
    {
        let Some(count) = NonZero::new(input.len()) else {
            return Ok(match input.need_more(None) {
                Some(need) => Parse::More(need),
                None => Parse::CapacityExhausted,
            });
        };
        let (head, consumed) = input.try_prefix(count)?;
        Ok(Parse::Item { head, consumed })
    }
}

impl eof::Finish for Bytes {
    fn finish<'id, 'input, 'd, R: buffer::Cursor>(
        &self,
        input: Input<'id, 'input, 'd, R>,
    ) -> End<'input, 'd, Self, R>
    where
        'd: 'input,
    {
        if input.is_empty() {
            Ok(Finish::Empty)
        } else {
            Ok(Finish::Item(
                input
                    .try_plan(0..input.len())
                    .map_err(scope::Error::Range)?,
            ))
        }
    }
}

const _: () = assert!(size_of::<Bytes>() == 0);
