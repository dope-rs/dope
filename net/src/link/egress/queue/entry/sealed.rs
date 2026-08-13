use dope_core::io::socket::msg;

use crate::link::egress::{data, queue::entry};

pub(in crate::link::egress) trait Prepare<'d> {
    fn prepare(&self, offset: usize, cap: usize) -> Option<entry::Part>;
}

impl<'d, B: data::Payload<'d>> Prepare<'d> for entry::Entry<B> {
    fn prepare(&self, offset: usize, cap: usize) -> Option<entry::Part> {
        let bytes = self.0.as_ref().get(offset..)?;
        let available = bytes.len();
        let take = available.min(cap);
        // SAFETY: the retained queue cannot retire this entry before release.
        let iovec = unsafe { msg::raw::Iovec::retain(&bytes[..take]) };
        Some(entry::Part { iovec, available })
    }
}
