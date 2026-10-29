use dope_sys::io::reactor::retry;

use crate::output::Data;

pub(super) enum Kind<C: retry::Context> {
    Data(Data<C>),
    Connect,
}
