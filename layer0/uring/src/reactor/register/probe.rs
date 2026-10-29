use std::mem;

use dope_sys::kernel;

use crate::{Register, error, reactor::op};

const COUNT: usize = 256;
const SUPPORTED: u16 = 1;

#[repr(C)]
struct Header {
    last: u8,
    len: u8,
    reserved: u16,
    reserved2: [u32; 3],
}

#[derive(Clone, Copy)]
#[repr(C)]
struct Entry {
    opcode: u8,
    reserved: u8,
    flags: u16,
    reserved2: u32,
}

#[repr(C)]
pub(crate) struct Probe {
    header: Header,
    entries: [Entry; COUNT],
}

impl Probe {
    pub(crate) fn acquire<T>(
        registry: &mut Register<'_, T>,
    ) -> Result<Self, kernel::Errno<error::Register<8>>> {
        use crate::reactor::ring::ffi;

        let entry = Entry {
            opcode: 0,
            reserved: 0,
            flags: 0,
            reserved2: 0,
        };
        let mut probe = Self {
            header: Header {
                last: 0,
                len: 0,
                reserved: 0,
                reserved2: [0; 3],
            },
            entries: [entry; COUNT],
        };
        registry.fd.register(ffi::REGISTER_PROBE, &mut probe)?;
        Ok(probe)
    }

    pub(crate) fn require(&self, opcode: op::Code) -> Result<(), op::Code> {
        let index = opcode.index();
        if index <= usize::from(self.header.last) && self.entries[index].flags & SUPPORTED != 0 {
            Ok(())
        } else {
            Err(opcode)
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Header>() == 16);
    assert!(mem::size_of::<Entry>() == 8);
    assert!(mem::size_of::<Probe>() == 16 + COUNT * 8);
};
