use dope_sys::{io::reactor::read, memory};

use crate::{
    reactor::input::{Arm, Call, entry::Id},
    store::buffer::Pool,
};

pub struct Poll<'a, C: read::Context, L: memory::raw::Layout, E, P> {
    pub(in crate::reactor::input) arm: Arm<'a, C, E, P>,
    pub(in crate::reactor::input) pool: &'a Pool<L>,
    pub(in crate::reactor::input) id: Id,
    pub(in crate::reactor::input) call: Call<C>,
}
