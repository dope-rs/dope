use std::{cell::Cell, pin::Pin, rc::Rc};

use dope::core::driver::{
    retained,
    schedule::{app::step, ready},
};
use dope_quic::{
    conn::id,
    proto::cid,
    runtime::{endpoint::config::Config, mux::app},
};
use o3::{buffer::resident, mem::quota};

use super::runtime::pooled::{Capture, Errors};

pub const CONFIG: Config = Config {
    max_conns: 1,
    outgoing_capacity: 2,
    outgoing_bytes_capacity: 2400,
    packet_buffer_slots: 2,
    packet_buffer_bytes: 1200,
    cid_rotation: cid::Rotation::disabled(),
    opening_lifetime: None,
};

pub fn capture<'d, const ID: u8>() -> Capture<'d, ID> {
    Capture {
        established: Rc::new(Cell::new(None)),
        errors: Rc::new(Errors::default()),
        received: Rc::new(Cell::new(false)),
        stop: Rc::new(Cell::new(None)),
    }
}

pub struct Idle;

impl<'d> app::Driver<'d, Capture<'d, 0>, 0, Vec<u8>, resident::Shared<'d>> for Idle {
    fn has_work(&self, _: Pin<&Capture<'d, 0>>) -> bool {
        false
    }

    fn drive<'scope, 'turn, C>(
        &mut self,
        _: app::Turn<'scope, Capture<'d, 0>, C>,
        _: ready::Target<'d>,
        _: dope::core::driver::schedule::app::Work<'turn, 'd>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> quota::Admission<step::Flow>
    where
        C: app::Set<Capture<'d, 0>, 0, Vec<u8>, resident::Shared<'d>, id::Driver<'d>>,
    {
        quota::Admission::Empty
    }
}
