use o3::buffer::Shared;

use super::Ctx;

pub trait Codec {
    type Head;
    type ParseState: Default;

    fn parse(&self, state: &mut Self::ParseState, buf: &Shared) -> Option<(Self::Head, usize)>;
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Close {
    Keep,
    Reconnect,
    Permanent,
}

pub trait Lifecycle: Default {
    fn wants_close(&self) -> Close;

    fn defer_close(&self) -> bool;

    fn is_drained(&self) -> bool;
}

#[derive(Default)]
pub struct Stateless;

impl Lifecycle for Stateless {
    fn wants_close(&self) -> Close {
        Close::Keep
    }

    fn defer_close(&self) -> bool {
        false
    }

    fn is_drained(&self) -> bool {
        true
    }
}

pub trait Session: Sized {
    type Codec: Codec;
    type ConnState: Lifecycle;

    fn codec(&self) -> &Self::Codec;

    fn connect(&mut self, ctx: &mut Ctx<'_, Self>);

    fn response(&mut self, head: <Self::Codec as Codec>::Head, ctx: &mut Ctx<'_, Self>);

    fn disconnect(&mut self, ctx: &mut Ctx<'_, Self>);

    fn flush_trailer(&mut self, ctx: &mut Ctx<'_, Self>) {
        let _ = ctx;
    }
}
