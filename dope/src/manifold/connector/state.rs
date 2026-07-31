use std::marker::PhantomData;
use std::time::Instant;

use o3::buffer::Shared;

use crate::manifold::connector::source::DialKey;
use dope_net::link::raw::core::{Establish, Outbound};
use dope_net::link::slot::PendingFlags;

pub const IOV_CAP: usize = 32;

pub struct State<C: Default, B: AsRef<[u8]> = Shared> {
    pub conn: C,
    pub(super) lane: usize,
    pub(super) dial: DialKey,
    pub(super) pending: PendingFlags,
    pub(super) establish: Establish,
    pub(super) retired: bool,
    /// Last inbound timestamp used to detect a silent peer.
    pub(super) last_recv: Option<Instant>,
    /// Whether the application requested a non-reconnecting close.
    pub(super) close_permanent: bool,
    _send: PhantomData<fn(B)>,
}

impl<C: Default, B: AsRef<[u8]>> Outbound for State<C, B> {
    fn establish(&mut self) -> &mut Establish {
        &mut self.establish
    }
}

impl<C: Default, B: AsRef<[u8]>> State<C, B> {
    pub(super) fn new(dial: DialKey, lane: usize) -> Self {
        Self {
            conn: C::default(),
            lane,
            dial,
            pending: PendingFlags::default(),
            establish: Establish::Idle,
            retired: false,
            last_recv: None,
            close_permanent: false,
            _send: PhantomData,
        }
    }
}
