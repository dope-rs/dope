use std::mem;

use dope_core::driver::route::{Identity, KeyTag};
use o3::buffer::resident;

use crate::network::listener::{conn, runtime::accept, writer::state};

pub(crate) struct State<'p, 's, 'd, ID: Identity, C, B = resident::Shared<'d>> {
    pub(in crate::network::listener) conn: C,
    pub(in crate::network::listener) send: state::Send<'p, 's, 'd, ID, B>,
    pub(in crate::network::listener) admission: Option<accept::Admission<'d, ID>>,
    pub(in crate::network::listener) failure: Option<conn::Failure>,
}

impl<'p, 's, 'd, ID: Identity, C, B> State<'p, 's, 'd, ID, C, B> {
    pub(in crate::network::listener) fn new(
        conn: C,
        admit: Option<accept::Admission<'d, ID>>,
    ) -> Self {
        Self {
            conn,
            send: state::Send::default(),
            admission: admit,
            failure: None,
        }
    }

    pub(in crate::network::listener) fn fail(&mut self, failure: conn::Failure) {
        self.failure.get_or_insert(failure);
    }
}

const _: () = {
    assert!(mem::size_of::<Option<conn::Failure>>() == 6 * mem::size_of::<usize>());
    assert!(
        mem::size_of::<State<'static, 'static, 'static, KeyTag<0>, ()>>()
            == mem::size_of::<state::Send<'static, 'static, 'static, KeyTag<0>>>()
                + mem::size_of::<Option<accept::Admission<'static, KeyTag<0>>>>()
                + mem::size_of::<Option<conn::Failure>>()
    );
    assert!(
        mem::size_of::<State<'static, 'static, 'static, KeyTag<0>, ()>>()
            == 13 * mem::size_of::<usize>()
    );
};
