use crate::backend;

pub const WRITE_BUF_CAP: usize = 16 * 1024;

#[repr(transparent)]
pub struct Buf([u8; WRITE_BUF_CAP]);

impl Default for Buf {
    fn default() -> Self {
        Self([0; WRITE_BUF_CAP])
    }
}

impl Buf {
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

#[derive(Default)]
pub enum SendSource {
    #[default]
    None,
    Static(&'static [u8]),
    Shared(o3::buffer::Shared),
}

impl SendSource {
    pub fn body(&self) -> &[u8] {
        match self {
            Self::None => &[],
            Self::Static(s) => s,
            Self::Shared(s) => s.as_ref(),
        }
    }
}

pub struct State {
    pub(super) arena: Option<super::arena::Handle>,
    pub write_buf_len: usize,
    pub armed_plain: usize,
    pub sent_plain: usize,
    pub total_plain: usize,
    pub source: SendSource,
    pub pending_iovs: [backend::socket::IoVec; 4],
    pub pending_msghdr: backend::socket::MsgHdr,
}

impl Default for State {
    fn default() -> Self {
        Self {
            arena: None,
            write_buf_len: 0,
            armed_plain: 0,
            sent_plain: 0,
            total_plain: 0,
            source: SendSource::None,
            pending_iovs: [backend::socket::IoVec::empty(); 4],
            pending_msghdr: backend::socket::MsgHdr::empty(),
        }
    }
}

impl State {
    #[inline]
    pub(super) fn begin(&mut self, total_plain: usize, source: SendSource) {
        self.total_plain = total_plain;
        self.sent_plain = 0;
        self.armed_plain = 0;
        self.source = source;
    }

    #[inline]
    pub(super) fn reset(&mut self) {
        self.write_buf_len = 0;
        self.total_plain = 0;
        self.sent_plain = 0;
        self.armed_plain = 0;
        self.source = SendSource::None;
    }

    /// `armed_plain` tracks only the submit that put a send on the wire, so a
    /// `Reclaim::OnComplete` CQE can be checked against what it covers.
    #[inline]
    pub(super) fn record_handoff(&mut self, consumed: usize, armed: bool) {
        if armed {
            self.armed_plain = consumed;
        }
        self.sent_plain += consumed;
    }
}
