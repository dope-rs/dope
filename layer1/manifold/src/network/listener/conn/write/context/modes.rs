use std::ops::ControlFlow;

use conn::write;
use dope_net::link::egress;
use egress::reserve::{self, error::Full, limit::Limit};
use write::error;

use crate::network::listener::conn;

impl<H, B, R> write::Commit<H, egress::NonEmpty<B>, R> {
    pub(super) fn try_bounds(&self) -> Result<reserve::Bounds, error::Bounded> {
        let header =
            Limit::at_most::<{ write::Prefix::CAPACITY }, { write::Prefix::CAPACITY as u32 }>();
        let Some(body) = self.body.as_ref() else {
            return Ok(reserve::Bounds::one(header));
        };
        let body = Limit::from(body);
        if self.written.nonempty().is_some() {
            reserve::Bounds::pair(header, body)
                .map_err(Full::Pair)
                .map_err(error::Bounded::Geometry)
        } else {
            Ok(reserve::Bounds::one(body))
        }
    }

    pub(super) fn split(self) -> ControlFlow<(R, write::End), Self> {
        if self.total.is_some() {
            return ControlFlow::Continue(self);
        }
        ControlFlow::Break((self.receipt, self.end))
    }
}
