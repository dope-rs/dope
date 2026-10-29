use o3::buffer::{self, hold};

use crate::client::session::codec;

/// A stateless codec that interprets staged bytes at transport EOF.
pub trait Finish: codec::Stateless {
    fn finish<'id, 'input, 'd, R: buffer::Cursor + hold::raw::Retain<'d>>(
        &self,
        input: codec::Input<'id, 'input, 'd, R>,
    ) -> codec::End<'input, 'd, Self, R>
    where
        'd: 'input;
}
