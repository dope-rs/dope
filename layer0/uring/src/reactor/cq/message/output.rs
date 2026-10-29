use std::mem;

use dope_sys::io::reactor::complete::message::{self, Cut};

use crate::reactor::cq::message::{Capacity, Error, Length, Span};

const PREFIX: usize = 4 * mem::size_of::<u32>();

pub struct Output<'a, const NAME: usize, const CONTROL: usize> {
    name: Span<'a>,
    payload: Span<'a>,
}

impl<'a, const NAME: usize, const CONTROL: usize> message::Output<'a>
    for Output<'a, NAME, CONTROL>
{
    type Capacity = Capacity;
    type Length = Length;

    fn parse(buffer: &'a [u8]) -> Result<Self, Error> {
        let Some(required) = PREFIX
            .checked_add(NAME)
            .and_then(|len| len.checked_add(CONTROL))
        else {
            return Err(Error::Overflow {
                name: Capacity::from_usize(NAME),
                control: Capacity::from_usize(CONTROL),
            });
        };
        if buffer.len() < required {
            return Err(Error::Short {
                actual: Length::from_usize(buffer.len()),
                required: Length::from_usize(required),
            });
        }
        let name_len = Span::decode([buffer[0], buffer[1], buffer[2], buffer[3]]);
        let ctrl_len = Span::decode([buffer[4], buffer[5], buffer[6], buffer[7]]);
        let data_len = Span::decode([buffer[8], buffer[9], buffer[10], buffer[11]]);
        let flags = u32::from_ne_bytes([buffer[12], buffer[13], buffer[14], buffer[15]]);
        let name = Span::name(&buffer[PREFIX..PREFIX + NAME], name_len);
        Span::validate(
            &buffer[PREFIX + NAME..required],
            ctrl_len,
            flags & libc::MSG_CTRUNC as u32 != 0,
            message::Region::Control,
        )?;
        let payload = Span::flagged(
            &buffer[required..],
            data_len,
            flags & libc::MSG_TRUNC as u32 != 0,
            message::Region::Payload,
        )?;
        Ok(Output { name, payload })
    }

    fn name_cut(&self) -> Cut {
        self.name.cut
    }

    fn payload_cut(&self) -> Cut {
        self.payload.cut
    }

    fn name(&self) -> &'a [u8] {
        self.name.bytes
    }

    fn payload(&self) -> &'a [u8] {
        self.payload.bytes
    }
}
