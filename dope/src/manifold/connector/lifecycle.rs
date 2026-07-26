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
