use dope_core::{driver::schedule::clock, io::socket::option};
use o3::base::time::Positive;

use crate::link::oneshot;

#[must_use = "a one-shot request must be submitted or recovered"]
pub struct Request<'d, A, P, B> {
    pub(super) address: P,
    pub(super) options: option::Set,
    pub(super) authority: A,
    pub(super) payload: B,
    pub(super) connect_deadline: clock::Deadline<'d>,
    pub(super) send_window: Positive,
}

impl<'d, A, P, B> Request<'d, A, P, B> {
    pub const fn new(
        address: P,
        options: option::Set,
        owner: A,
        payload: B,
        until: clock::Deadline<'d>,
        window: Positive,
    ) -> Self {
        Self {
            address,
            options,
            authority: owner,
            payload,
            connect_deadline: until,
            send_window: window,
        }
    }

    pub fn into_parts(self) -> oneshot::Parts<'d, A, P, B> {
        oneshot::Parts {
            address: self.address,
            options: self.options,
            authority: self.authority,
            payload: self.payload,
            connect_deadline: self.connect_deadline,
            send_window: self.send_window,
        }
    }
}
