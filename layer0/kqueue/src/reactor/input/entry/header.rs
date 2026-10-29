use dope_sys::io::reactor::read;

use crate::reactor::input::File;

#[derive(Clone, Copy)]
pub(in crate::reactor::input) struct Header<C: read::Context> {
    pub(in crate::reactor::input) file: File<C>,
    pub(in crate::reactor::input) key: C::Key,
}
