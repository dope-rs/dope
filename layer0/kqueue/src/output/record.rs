use dope_sys::io::reactor::retry;

use crate::output::{File, Kind};

pub struct Record<C: retry::Context> {
    pub(super) file: File<C>,
    pub(super) kind: Kind<C>,
}
