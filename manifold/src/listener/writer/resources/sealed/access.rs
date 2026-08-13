use std::pin;

use dope_core::io::{socket::msg, transfer};
use dope_net::{link::egress::data, wire::send};

use crate::listener::writer::{flow, resources};

pub(in crate::listener) trait Access<'a> {
    fn plain(self, written: flow::Written) -> send::Plain<'a>;
    fn vectored(self, cursor: flow::PlainCursor, limit: transfer::Len) -> send::Vectored<'a>;
}

impl<'a, 'd, const ID: u8> Access<'a> for pin::Pin<&'a mut resources::Flight<'d, ID>> {
    fn plain(self, written: flow::Written) -> send::Plain<'a> {
        let this = self.project();
        let bytes = written.prefix(this.header.into_ref().as_slice());
        // SAFETY: Flight and its pool lease remain pinned through completion.
        unsafe { send::raw::Plain::retain(bytes) }
    }

    fn vectored(self, cursor: flow::PlainCursor, limit: transfer::Len) -> send::Vectored<'a> {
        let this = self.project();
        // SAFETY: Send constructs a cursor bounded by accepted progress.
        let header = unsafe {
            this.header
                .into_ref()
                .as_slice()
                .get_unchecked(cursor.header_start()..cursor.header_end())
        };
        let source: &Option<data::Buffer<'d>> = this.source;
        let body = source
            .as_ref()
            .map_or(&[][..], |source: &data::Buffer<'d>| source.as_ref());
        // SAFETY: consumed progress bounds the cursor against the retained source.
        let body = unsafe { body.get_unchecked(cursor.body_start()..) };
        let parts = msg::Parts::prefixes(limit, [header, body]);
        let message = msg::Builder::new(this.message).finish(this.iovecs, parts);
        send::Vectored::from_message(message)
    }
}
