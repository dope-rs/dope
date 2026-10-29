use dope_core::io::event::{batch::Flow, net::recv};

use crate::link::event;

pub(in crate::link::pool::recv) enum Timing {
    Fresh(Flow),
    Replay,
}

#[derive(Clone, Copy)]
pub(in crate::link::pool::recv) enum Origin {
    Fresh,
    Replay,
}

/// A completion retaining whether its kernel flight still needs observation.
/// Only receive dispatch can separate the payload from that authority.
pub struct Input<T> {
    value: T,
    timing: Timing,
}

impl<T> Input<T> {
    pub(in crate::link::pool::recv) fn into_parts(self) -> (T, Timing) {
        (self.value, self.timing)
    }
}

pub trait Arrival<'d> {
    type Observed;

    fn observe(self) -> Input<Self::Observed>;
}

impl<'d> Arrival<'d> for recv::Data<'d> {
    type Observed = event::Data<'d>;

    fn observe(self) -> Input<Self::Observed> {
        let (token, more, lease) = self.into_parts();
        Input {
            value: event::Data::new(token, lease),
            timing: Timing::Fresh(more),
        }
    }
}

impl<'d> Arrival<'d> for event::Data<'d> {
    type Observed = Self;

    fn observe(self) -> Input<Self::Observed> {
        Input {
            value: self,
            timing: Timing::Replay,
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
            timing: Timing::Replay,
        }
    }
}
