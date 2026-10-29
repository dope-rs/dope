use std::{mem, ops::ControlFlow};

use conn::write;
use dope_core::{driver::route::KeyTag, io::transfer};
use dope_net::link::egress::{self, load::Value};
use egress::reserve::{
    self,
    error::{Full, Part},
    limit::Limit,
};
use o3::buffer::resident;
use write::error;

use crate::network::listener::{conn, writer::memory};

pub(super) struct Bounded<H, B, R> {
    header: H,
    draft: write::Draft<B, R>,
    written: memory::Len,
    total: Option<transfer::NonZero>,
}

impl<H, B, R> Bounded<H, B, R> {
    pub(super) fn try_new(
        header: H,
        draft: write::Draft<B, R>,
    ) -> Result<Self, error::Mismatch<B, R>> {
        let checked = (|| {
            let written = memory::Len::try_new(draft.written()).map_err(error::Bounded::Prefix)?;
            let total = match draft.body() {
                Some(Value::Checked(body)) => Some(
                    body.geometry()
                        .wire()
                        .checked_add(written.transfer())
                        .map_err(egress::WireError::Total)
                        .map_err(error::Bounded::Wire)?,
                ),
                Some(Value::Unchecked(body)) => body
                    .charge()
                    .bytes()
                    .try_total(written.transfer())
                    .map_err(error::Bounded::Wire)?,
                None => written.transfer().nonzero(),
            };
            Ok((written, total))
        })();
        match checked {
            Ok((written, total)) => Ok(Self {
                header,
                draft,
                written,
                total,
            }),
            Err(cause) => Err(error::Mismatch::bounded(cause, draft)),
        }
    }

    pub(super) fn try_bounds(&self) -> Result<reserve::Bounds, error::Bounded> {
        let header =
            Limit::at_most::<{ write::Prefix::CAPACITY }, { write::Prefix::CAPACITY as u32 }>();
        let Some(body) = self.draft.body() else {
            return Ok(reserve::Bounds::one(header));
        };
        if matches!(body, Value::Unchecked(body) if body.charge().bytes() == egress::WireBytes::ZERO)
        {
            return Ok(reserve::Bounds::one(header));
        }
        let part = if self.written.nonempty().is_some() {
            Part::Second
        } else {
            Part::One
        };
        let checked = match body {
            Value::Checked(body) => Ok(Limit::from(body)),
            Value::Unchecked(body) => Limit::try_exact(body.charge()),
        };
        let body = match checked {
            Ok(body) => body,
            Err(cause) => {
                return Err(error::Bounded::Geometry(Full::Limit { part, cause }));
            }
        };
        if self.written.nonempty().is_some() {
            reserve::Bounds::pair(header, body)
                .map_err(Full::Pair)
                .map_err(error::Bounded::Geometry)
        } else {
            Ok(reserve::Bounds::one(body))
        }
    }

    pub(super) fn split(self) -> ControlFlow<(R, write::End), Self> {
        if self.total.is_some() || self.draft.body().is_some() {
            return ControlFlow::Continue(self);
        }
        let (_, _, receipt, end) = self.draft.into_parts();
        ControlFlow::Break((receipt, end))
    }

    pub(super) fn into_draft(self) -> write::Draft<B, R> {
        self.draft
    }

    pub(super) fn validate(self) -> write::Commit<H, B, R> {
        let (_, body, receipt, end) = self.draft.into_parts();
        write::Commit {
            header: self.header,
            written: self.written,
            total: self.total,
            body,
            receipt,
            end,
        }
    }
}

const _: () = {
    let word = mem::size_of::<usize>();
    assert!(
        mem::size_of::<
            Bounded<
                memory::HeaderSlot<'static, 'static, 'static, KeyTag<0>>,
                resident::Shared<'static>,
                (),
            >,
        >() == 11 * word
    );
    assert!(
        mem::size_of::<
            ControlFlow<
                ((), write::End),
                Bounded<
                    memory::HeaderSlot<'static, 'static, 'static, KeyTag<0>>,
                    resident::Shared<'static>,
                    (),
                >,
            >,
        >() == 11 * word
    );
};
