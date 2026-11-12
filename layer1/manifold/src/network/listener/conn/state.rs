use std::{mem, ops};

use dope_core::driver::route::{Identity, KeyTag};
use dope_net::link::egress::data;
use o3::buffer::resident;

use crate::network::listener::{conn, runtime::accept, writer::state};

pub(crate) struct State<
    'p,
    's,
    'd,
    ID: Identity,
    C,
    B: data::raw::Payload<'d> = resident::Shared<'d>,
> {
    pub(in crate::network::listener) conn: C,
    pub(in crate::network::listener) admission: Option<accept::Admission<'d, ID>>,
    pub(in crate::network::listener) control: Control<'p, 's, 'd, ID, B>,
}

pub(crate) struct Control<'p, 's, 'd, ID: Identity, B: data::raw::Payload<'d>> {
    pub(in crate::network::listener) send: state::Send<'p, 's, 'd, ID, B>,
    pub(in crate::network::listener) failure: Option<conn::Failure>,
}

impl<'p, 's, 'd, ID: Identity, C, B: data::raw::Payload<'d>> State<'p, 's, 'd, ID, C, B> {
    pub(in crate::network::listener) fn new(
        conn: C,
        admit: Option<accept::Admission<'d, ID>>,
    ) -> Self {
        Self {
            conn,
            admission: admit,
            control: Control {
                send: state::Send::default(),
                failure: None,
            },
        }
    }
}

impl<'d, ID: Identity, B: data::raw::Payload<'d>> Control<'_, '_, 'd, ID, B> {
    pub(in crate::network::listener) fn fail(&mut self, failure: conn::Failure) {
        self.failure.get_or_insert(failure);
    }
}

impl<'p, 's, 'd, ID: Identity, C, B: data::raw::Payload<'d>> ops::Deref
    for State<'p, 's, 'd, ID, C, B>
{
    type Target = Control<'p, 's, 'd, ID, B>;

    fn deref(&self) -> &Self::Target {
        &self.control
    }
}

impl<'d, ID: Identity, C, B: data::raw::Payload<'d>> ops::DerefMut for State<'_, '_, 'd, ID, C, B> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.control
    }
}

const _: () = {
    assert!(mem::size_of::<crate::app::receive::Failure>() == 6 * mem::size_of::<usize>());
    assert!(mem::size_of::<Option<conn::Failure>>() == 7 * mem::size_of::<usize>());
    assert!(
        mem::size_of::<State<'static, 'static, 'static, KeyTag<0>, ()>>()
            == 13 * mem::size_of::<usize>()
    );
};
