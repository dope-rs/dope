use std::mem;

use crate::{
    Errno,
    io::socket::record::{Fault, Record, Size, Transport},
};

const PAYLOAD: usize = mem::size_of::<u32>();
const PROBE: usize = PAYLOAD + 1;

/// Stream frames retain one length byte followed by at most 255 payload bytes.
/// Invalid lengths are drained before reporting their bounded observed extent.
pub struct Stream;

impl Transport for Stream {
    fn send(
        value: u32,
        mut send: impl for<'a> FnMut(&'a [u8]) -> Result<&'a [u8], Errno>,
    ) -> Result<(), Fault> {
        let [a, b, c, d] = value.to_ne_bytes();
        let bytes = [PAYLOAD as u8, a, b, c, d];
        let mut pending = bytes.as_slice();
        while !pending.is_empty() {
            let sent = match send(pending) {
                Ok(sent) if !sent.is_empty() => sent.len(),
                Ok(_) => return Err(size((bytes.len() - pending.len()).saturating_sub(1))),
                Err(error) if error.is_intr() => continue,
                Err(error) => return Err(Fault::Syscall(error)),
            };
            pending = pending.get(sent..).ok_or_else(|| size(sent))?;
        }
        Ok(())
    }

    fn recv(mut recv: impl for<'a> FnMut(&'a mut [u8]) -> Result<&'a mut [u8], Errno>) -> Record {
        match receive(&mut recv) {
            Ok(record) => record,
            Err(error) => Record::Failed(error),
        }
    }
}

fn receive(
    recv: &mut impl for<'a> FnMut(&'a mut [u8]) -> Result<&'a mut [u8], Errno>,
) -> Result<Record, Fault> {
    let mut header = [0];
    if fill(&mut header, recv)? == 0 {
        return Ok(Record::Closed);
    }
    let length = usize::from(header[0]);
    if length == PAYLOAD {
        let mut bytes = [0; PAYLOAD];
        let received = fill(&mut bytes, recv)?;
        return if received == PAYLOAD {
            Ok(Record::Value(u32::from_ne_bytes(bytes)))
        } else {
            Err(size(received))
        };
    }
    let mut bytes = [0; PROBE];
    let mut pending = length;
    while pending != 0 {
        let extent = pending.min(bytes.len());
        let received = fill(&mut bytes[..extent], recv)?;
        pending -= received;
        if received != extent {
            break;
        }
    }
    Err(size(length - pending))
}

/// Retains the unfilled suffix across short completions and interrupted calls.
fn fill(
    mut output: &mut [u8],
    recv: &mut impl for<'a> FnMut(&'a mut [u8]) -> Result<&'a mut [u8], Errno>,
) -> Result<usize, Fault> {
    let capacity = output.len();
    while !output.is_empty() {
        let received = match recv(&mut *output) {
            Ok(received) if !received.is_empty() => received.len(),
            Ok(_) => break,
            Err(error) if error.is_intr() => continue,
            Err(error) => return Err(Fault::Syscall(error)),
        };
        output = output.get_mut(received..).ok_or_else(|| size(received))?;
    }
    Ok(capacity - output.len())
}

fn size(actual: usize) -> Fault {
    Fault::Record(Size {
        expected: PAYLOAD,
        actual: actual.min(PROBE),
    })
}
