use dope_core::io::event::{batch::Flow, net::recv};

use crate::link::event;

pub(in crate::link::pool::recv) enum Timing<R = ()> {
    Fresh(Flow),
    Replay(R),
}

#[derive(Clone, Copy)]
pub(in crate::link::pool::recv) enum Origin {
    Fresh,
    Replay,
}

/// A completion retaining whether its kernel flight still needs observation.
/// Only receive dispatch can separate the payload from that authority.
pub struct Input<T, R = ()> {
    value: T,
    timing: Timing<R>,
}

impl<T, R> Input<T, R> {
    pub(in crate::link::pool::recv) fn into_parts(self) -> (T, Timing<R>) {
        (self.value, self.timing)
    }
}

pub trait Arrival<'d, R = ()> {
    type Observed;

    fn observe(self) -> Input<Self::Observed, R>;
}

impl<'d, R> Arrival<'d, R> for recv::Data<'d> {
    type Observed = event::Data<'d>;

    fn observe(self) -> Input<Self::Observed, R> {
        let (token, more, lease) = self.into_parts();
        Input {
            value: event::Data::new(token, lease),
            timing: Timing::Fresh(more),
        }
    }
}

impl<'d, R> Arrival<'d, R> for event::Data<'d, R> {
    type Observed = event::Data<'d>;

    fn observe(self) -> Input<Self::Observed, R> {
        let (value, receipt) = self.into_parts();
        Input {
            value,
            timing: Timing::Replay(receipt),
        }
    }
}

impl<'d> Arrival<'d> for recv::Control<'d> {
    type Observed = event::Control<'d>;

    fn observe(self) -> Input<Self::Observed> {
        let more = self.flow();
        Input {
            value: event::Control::new(self),
            timing: Timing::Fresh(more),
        }
    }
}

impl<'d> Arrival<'d> for event::Control<'d> {
    type Observed = Self;

    fn observe(self) -> Input<Self::Observed> {
        Input {
            value: self,
            timing: Timing::Replay(()),
        }
    }
}
