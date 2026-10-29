use std::mem;

use dope_sys::Errno;

#[derive(Debug)]
pub enum Wait {
    Read(Errno),
}

const _: () = {
    assert!(mem::size_of::<Wait>() == mem::size_of::<Errno>());
    assert!(mem::align_of::<Wait>() == mem::align_of::<Errno>());
    assert!(mem::size_of::<Result<(), Wait>>() == mem::size_of::<Errno>());
};
