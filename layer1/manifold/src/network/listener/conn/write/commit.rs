use std::{convert::Infallible, mem};

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
    pub(in crate::network::listener::conn::write) body: Option<B>,
    pub(in crate::network::listener::conn::write) receipt: R,
    pub(in crate::network::listener::conn::write) end: write::End,
}

impl<'id, B, R> Commit<region::Identity<'id>, Value<B>, R> {
    pub(in crate::network::listener::conn::write) fn new(
        header: region::Identity<'id>,
        written: memory::Len,
        body: Option<Value<B>>,
        receipt: R,
    ) -> Self {
        Self {
            header,
            written,
            total: None,
            body,
            receipt,
            end: write::End::Open,
        }
    }

    fn try_bind<H>(
        mut self,
        header: region::Value<region::Identity<'id>, H>,
    ) -> Result<Commit<H, egress::NonEmpty<B>, R>, error::Mismatch<B, R>> {
        let body = match self.body.take() {
            Some(Value::Unchecked(body)) => match body.try_nonempty() {
                Ok(body) => Some(body),
                Err(rejected) => match egress::Empty::try_from(rejected) {
                    Ok(_) => None,
                    Err(rejected) => {
                        let (body, cause) = rejected.into_parts();
                        self.body = Some(Value::Unchecked(body));
                        return Err(self.reject(error::Bounded::Payload(cause)));
                    }
                },
            },
            Some(Value::Checked(body)) => Some(body),
            None => None,
        };
        let total = match body.as_ref() {
            Some(value) => match value.geometry().wire().checked_add(self.written.transfer()) {
                Ok(total) => Some(total),
                Err(cause) => {
                    self.body = body.map(Value::Checked);
                    return Err(self.reject(error::Bounded::Wire(egress::WireError::Total(cause))));
                }
            },
            None => self.written.transfer().nonzero(),
        };
        Ok(Commit {
            header: header.into_inner(),
            written: self.written,
            total,
            body,
            receipt: self.receipt,
            end: self.end,
        })
    }
}

impl<H, B, R> Commit<H, egress::NonEmpty<B>, R> {
    pub(in crate::network::listener::conn::write) fn try_with<C, E>(
        state: &mut C,
        header: H,
        call: impl for<'b, 'id> FnOnce(
            &mut C,
            write::Prefix<'b, 'id>,
        ) -> Result<Commit<region::Identity<'id>, Value<B>, R>, E>,
    ) -> Result<Self, error::Error<B, R, E>>
    where
        H: AsMut<[mem::MaybeUninit<u8>; memory::WRITE_BUF_CAP]>,
    {
        region::Token::scope(|token| {
            let mut header = token.bind(header);
            let prefix = write::Prefix::new(header.get_mut().as_mut(), token.proof());
            let commit = call(state, prefix).map_err(error::Error::Build)?;
            commit.try_bind(header).map_err(error::Error::Mismatch)
        })
    }
}

impl<H, B, R> Commit<H, B, R> {
    pub fn close_after(mut self) -> Self {
        self.end = write::End::Close;
        self
    }
    pub fn shutdown(mut self) -> Self {
        self.end = write::End::Shutdown;
        self
    }
    pub fn into_parts(self) -> (usize, Option<B>, R, write::End) {
        (
            self.written.transfer().into_raw(),
            self.body,
            self.receipt,
            self.end,
        )
    }
}

impl<H, B, R> Commit<H, Value<B>, R> {
    fn reject(self, cause: error::Bounded) -> error::Mismatch<B, R> {
        error::Mismatch::Bounded {
            cause,
            commit: Commit {
                header: (),
                written: self.written,
                total: self.total,
                body: self.body,
                receipt: self.receipt,
                end: self.end,
            },
        }
    }
}

impl<H, B, R> Commit<H, egress::NonEmpty<B>, R> {
    pub(in crate::network::listener::conn::write) fn reject(
        self,
        cause: error::Bounded,
    ) -> error::Mismatch<B, R> {
        error::Mismatch::Bounded {
            cause,
            commit: Commit {
                header: (),
                written: self.written,
                total: self.total,
                body: self.body.map(Value::Checked),
                receipt: self.receipt,
                end: self.end,
            },
        }
    }
}

impl<H, R> Commit<H, Value<Infallible>, R> {
    pub(in crate::network::listener::conn::write) fn into_empty<B>(self) -> Commit<H, Value<B>, R> {
        if let Some(body) = self.body {
            match *body.as_inner() {}
        }
        Commit {
            header: self.header,
            written: self.written,
            total: self.total,
            body: None,
            receipt: self.receipt,
            end: self.end,
        }
    }
}

impl<'p, 's, 'd, ID: Identity, B: data::raw::Payload<'d>, R>
    Commit<memory::HeaderSlot<'p, 's, 'd, ID>, egress::NonEmpty<B>, R>
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
        let body = self
            .body
            .map(egress::NonEmpty::into_right)
            .map(Value::Checked);
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

impl<'p, 's, 'd, ID: Identity, B: data::raw::Payload<'d>, R>
    Commit<memory::DirectSlot<'p, 's, 'd, ID, B>, egress::NonEmpty<B>, R>
{
    pub(in crate::network::listener) fn direct(
        self,
    ) -> (R, write::End, Option<access::Direct<'p, 's, 'd, ID, B>>) {
        let entry = self.total.map(|total| {
            access::Direct::new(
                self.header,
                self.written,
                total,
                self.body.map(egress::NonEmpty::into_inner),
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
        mem::size_of::<Commit<region::Identity<'static>, Value<resident::Shared<'static>>, ()>>()
            == 8 * word
    );
    assert!(
        mem::size_of::<
            Commit<
                memory::HeaderSlot<'static, 'static, 'static, KeyTag<0>>,
                egress::NonEmpty<resident::Shared<'static>>,
                (),
            >,
        >() == 9 * word
    );
    assert!(
        mem::size_of::<(
            (),
            write::End,
            Option<access::Direct<'static, 'static, 'static, KeyTag<0>, resident::Shared<'static>>>
        )>() == 6 * word
    );
};
