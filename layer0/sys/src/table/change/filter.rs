/// Descriptor filter whose pending change lost its owning row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Filter {
    Read,
    Write,
}

const _: () = assert!(std::mem::size_of::<Filter>() == 1);
