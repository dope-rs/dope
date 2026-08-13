use std::time;

use dope_core::io::socket::{self, option};

use crate::connector::{connection, lifecycle};

pub struct State<C, O> {
    pub(super) conn: C,
    pub(super) owner: O,
    pub(super) peer: Option<socket::Addr>,
    pub(super) options: option::StreamOptions,
    pub(super) last_recv: Option<time::Instant>,
    pub(super) closing: connection::Closing,
}

impl<C, O> State<C, O> {
    pub(super) fn new(conn: C, owner: O, options: option::StreamOptions) -> Self {
        use connection::CloseState;

        Self {
            conn,
            owner,
            peer: None,
            options,
            last_recv: None,
            closing: connection::Closing {
                state: CloseState::Open,
                reason: None,
            },
        }
    }

    pub(super) fn request_close(
        &mut self,
        reason: lifecycle::CloseReason,
    ) -> lifecycle::CloseReason {
        self.closing.request(reason)
    }
}
