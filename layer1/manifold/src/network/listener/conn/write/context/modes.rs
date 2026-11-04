use std::ops::ControlFlow;

use conn::write;
use dope_net::link::egress::{self, load::Value};
use egress::reserve::{
    self,
    error::{Full, Part},
    limit::Limit,
};
use write::error;

use crate::network::listener::{conn, writer::memory};

impl<H, B, R> write::Commit<H, B, R> {
    pub(super) fn try_draft(
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
            Ok((written, total)) => {
                let (_, body, receipt, end) = draft.into_parts();
                Ok(Self {
                    header,
                    written,
                    total,
                    body,
                    receipt,
                    end,
                })
            }
            Err(cause) => Err(error::Mismatch::bounded(cause, draft)),
        }
    }

    pub(super) fn try_bounds(&self) -> Result<reserve::Bounds, error::Bounded> {
        let header =
            Limit::at_most::<{ write::Prefix::CAPACITY }, { write::Prefix::CAPACITY as u32 }>();
        let Some(body) = self.body.as_ref() else {
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
        if self.total.is_some() || self.body.is_some() {
            return ControlFlow::Continue(self);
        }
        ControlFlow::Break((self.receipt, self.end))
    }
}
