use crate::listener::{accept, writer::state};

pub struct State<'d, const ID: u8, C> {
    pub(in crate::listener) conn: C,
    pub(in crate::listener) send: state::Send<'d, ID>,
    pub(in crate::listener) peer_ip: Option<accept::Admission>,
}

impl<'d, const ID: u8, C> State<'d, ID, C> {
    pub(in crate::listener) fn new(conn: C, peer_ip: Option<accept::Admission>) -> Self {
        Self {
            conn,
            send: state::Send::default(),
            peer_ip,
        }
    }
}
