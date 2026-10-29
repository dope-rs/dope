use std::convert::Infallible;

use dope_net::link::egress::load::Value;

use crate::network::listener::conn::write;

/// Recoverable observation produced before a write contract is validated.
pub struct Draft<B, R> {
    written: usize,
    body: Option<Value<B>>,
    receipt: R,
    end: write::End,
}

impl<R> Draft<Infallible, R> {
    pub(in crate::network::listener::conn::write) fn into_empty<B>(self) -> Draft<B, R> {
        if let Some(body) = self.body {
            match *body.as_inner() {}
        }
        Draft {
            written: self.written,
            body: None,
            receipt: self.receipt,
            end: self.end,
        }
    }
}

impl<B, R> Draft<B, R> {
    pub fn new(written: usize, body: Option<Value<B>>, receipt: R) -> Self {
        Self {
            written,
            body,
            receipt,
            end: write::End::Open,
        }
    }

    pub fn close_after(mut self) -> Self {
        self.end = write::End::Close;
        self
    }

    pub fn shutdown(mut self) -> Self {
        self.end = write::End::Shutdown;
        self
    }

    pub(in crate::network::listener) const fn written(&self) -> usize {
        self.written
    }

    pub(in crate::network::listener) const fn body(&self) -> Option<&Value<B>> {
        self.body.as_ref()
    }

    pub fn into_parts(self) -> (usize, Option<Value<B>>, R, write::End) {
        (self.written, self.body, self.receipt, self.end)
    }
}
