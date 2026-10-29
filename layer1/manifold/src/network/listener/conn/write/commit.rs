use std::mem;

use data::{compose, exec};
use dope_core::{
    driver::route::{Identity, KeyTag},
    io::transfer,
};
use dope_net::link::egress::{self, data, load::Value};
use egress::reserve;
use memory::access;
use o3::{base::cell::region, buffer::resident};
use write::error;

use crate::network::listener::{self, conn::write, writer::memory};

/// A commit bound to its callback identity or its actual header owner.
pub struct Commit<H, B, R> {
    pub(in crate::network::listener::conn::write) header: H,
    pub(in crate::network::listener::conn::write) written: memory::Len,
    pub(in crate::network::listener::conn::write) total: Option<transfer::NonZero>,
    pub(in crate::network::listener::conn::write) body: Option<Value<B>>,
    pub(in crate::network::listener::conn::write) receipt: R,
    pub(in crate::network::listener::conn::write) end: write::End,
}

impl<'id, B, R> Commit<region::Proof<region::Identity<'id>>, B, R> {
    pub(in crate::network::listener) fn new(
        header: region::Proof<region::Identity<'id>>,
        written: memory::Len,
        total: Option<transfer::NonZero>,
        body: Option<Value<B>>,
        receipt: R,
    ) -> Self {
        Self {
            header,
            written,
            total,
            body,
            receipt,
            end: write::End::Open,
        }
    }

    pub(in crate::network::listener) fn bind<H>(
        self,
        header: region::Value<region::Identity<'id>, H>,
    ) -> Commit<H, B, R> {
        Commit {
            header: header.into_inner(),
            written: self.written,
            total: self.total,
            body: self.body,
            receipt: self.receipt,
            end: self.end,
        }
    }
}

impl<H, B, R> Commit<H, B, R> {
    pub fn close_after(mut self) -> Self {
        self.end = write::End::Close;
        self
    }
}

impl<'p, 's, 'd, ID: Identity, B: data::raw::Payload<'d>, R>
    Commit<memory::HeaderSlot<'p, 's, 'd, ID>, B, R>
{
    pub(in crate::network::listener) fn try_publish<'e>(
        self,
        vacant: reserve::Vacant<
            '_,
            '_,
            'e,
            'd,
            compose::Either<listener::raw::Owned<'p, 's, 'd, ID>, B>,
            <B::Mode as exec::Mode>::Reserve<{ memory::WRITE_BUF_CAP }>,
        >,
    ) -> Result<(R, write::End, reserve::Publish), error::Mismatch<B, R>> {
        let header = self.header.finish(self.written.nonempty());
        let header = header.map(egress::Load::new).map(egress::Load::into_left);
        let body = self.body.map(Value::into_right);
        let done = match vacant.try_publish(header.map(Value::Unchecked), body) {
            Ok(done) => done,
            Err(cause) => {
                let (cause, bounds, first, second) = cause.into_parts();
                return Err(error::Mismatch::values(
                    cause,
                    bounds,
                    part(first),
                    part(second),
                    self.receipt,
                    self.end,
                ));
            }
        };
        Ok((self.receipt, self.end, done))
    }
}

impl<'p, 's, 'd, ID: Identity, B, R> Commit<memory::DirectSlot<'p, 's, 'd, ID, B>, B, R> {
    pub(in crate::network::listener) fn direct(
        self,
    ) -> (R, write::End, Option<access::Direct<'p, 's, 'd, ID, B>>) {
        let entry = self.total.map(|total| {
            access::Direct::new(
                self.header,
                self.written,
                total,
                self.body.map(Value::into_inner),
            )
        });
        (self.receipt, self.end, entry)
    }
}

fn part<'p, 's, 'd, ID: Identity, B>(
    value: Option<Value<compose::Either<listener::raw::Owned<'p, 's, 'd, ID>, B>>>,
) -> error::Part<B> {
    let Some(value) = value else {
        return error::Part::Empty;
    };
    match value.split() {
        compose::Either::Left(prefix) => error::Part::Prefix(prefix.charge()),
        compose::Either::Right(body) => error::Part::Body(body),
    }
}

const _: () = {
    let word = mem::size_of::<usize>();
    assert!(
        mem::size_of::<
            Commit<region::Proof<region::Identity<'static>>, resident::Shared<'static>, ()>,
        >() == 8 * word
    );
    assert!(
        mem::size_of::<
            Commit<
                memory::HeaderSlot<'static, 'static, 'static, KeyTag<0>>,
                resident::Shared<'static>,
                (),
            >,
        >() == 10 * word
    );
    assert!(
        mem::size_of::<(
            (),
            write::End,
            Option<access::Direct<'static, 'static, 'static, KeyTag<0>, resident::Shared<'static>>>
        )>() == 6 * word
    );
};
