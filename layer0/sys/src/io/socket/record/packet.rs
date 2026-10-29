use std::mem;

use crate::{
    Errno,
    io::socket::record::{Fault, Record, Size, Transport},
};

/// Native sequenced packets retain each record's boundary in the kernel.
pub struct Packet;

impl Transport for Packet {
    fn send(
        value: u32,
        mut send: impl for<'a> FnMut(&'a [u8]) -> Result<&'a [u8], Errno>,
    ) -> Result<(), Fault> {
        let bytes = value.to_ne_bytes();
        loop {
            match send(&bytes) {
                Ok(sent) if sent.len() == bytes.len() => return Ok(()),
                Ok(sent) => {
                    return Err(Fault::Record(Size {
                        expected: bytes.len(),
                        actual: sent.len(),
                    }));
                }
                Err(error) if error.is_intr() => {}
                Err(error) => return Err(Fault::Syscall(error)),
            }
        }
    }

    fn recv(mut recv: impl for<'a> FnMut(&'a mut [u8]) -> Result<&'a mut [u8], Errno>) -> Record {
        const EXTENT: usize = mem::size_of::<u32>() + 1;
        let mut bytes = [0; EXTENT];
        loop {
            match recv(&mut bytes) {
                Ok([]) => return Record::Closed,
                Ok([a, b, c, d]) => return Record::Value(u32::from_ne_bytes([*a, *b, *c, *d])),
                Ok(received) => {
                    return Record::Failed(Fault::Record(Size {
                        expected: mem::size_of::<u32>(),
                        actual: received.len(),
                    }));
                }
                Err(error) if error.is_intr() => {}
                Err(error) => return Record::Failed(Fault::Syscall(error)),
            }
        }
    }
}
