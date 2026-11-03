use std::{cell::Cell, rc::Rc};

use dope_quic::{proto::cid, runtime::endpoint::config::Config};

use super::runtime::pooled::{Capture, Errors};

pub const CONFIG: Config = Config {
    max_conns: 1,
    outgoing_capacity: 2,
    outgoing_bytes_capacity: 2400,
    packet_buffer_slots: 2,
    packet_buffer_bytes: 1200,
    cid_rotation: cid::Rotation::disabled(),
};

pub fn capture<'d>() -> Capture<'d, 0> {
    Capture {
        established: Rc::new(Cell::new(None)),
        errors: Rc::new(Errors::default()),
        received: Rc::new(Cell::new(false)),
        stop: Rc::new(Cell::new(None)),
    }
}
