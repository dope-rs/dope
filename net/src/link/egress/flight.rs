use dope_core::driver::token::Token;
use dope_core::io::socket::msg::{IoVec, MsgHdr};

use crate::wire::send::{StableVectoredSource, Vectored};

pub(super) struct Flight<const IOV: usize> {
    active: bool,
    has_wire: bool,
    iovs: [IoVec; IOV],
    iov_storage: [IoVec; IOV],
    msghdr_storage: MsgHdr,
    len: usize,
    bytes: usize,
    target: Option<Token>,
}

impl<const IOV: usize> Flight<IOV> {
    pub(super) fn new() -> Self {
        Self {
            active: false,
            has_wire: false,
            iovs: [IoVec::empty(); IOV],
            iov_storage: [IoVec::empty(); IOV],
            msghdr_storage: MsgHdr::empty(),
            len: 0,
            bytes: 0,
            target: None,
        }
    }

    pub(super) fn begin(&mut self) {
        debug_assert!(!self.active);
        self.active = true;
        self.has_wire = false;
        self.len = 0;
        self.bytes = 0;
        self.target = None;
    }

    pub(super) fn finish(&mut self) {
        self.active = false;
        self.has_wire = false;
        self.len = 0;
        self.bytes = 0;
        self.target = None;
    }

    pub(super) fn is_active(&self) -> bool {
        self.active
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(super) fn push(&mut self, iov: IoVec) {
        self.bytes += iov.len();
        self.iovs[self.len] = iov;
        self.len += 1;
    }

    pub(super) fn saw_wire(&mut self) {
        self.has_wire = true;
    }

    pub(super) fn has_wire(&self) -> bool {
        self.active && self.has_wire
    }

    pub(super) fn mark(&mut self, target: Token) {
        debug_assert!(self.active && self.target.is_none());
        self.target = Some(target);
    }

    pub(super) fn matches(&self, target: Token) -> bool {
        self.target
            .is_some_and(|current| current.same_target(target))
    }

    pub(super) fn vectored(&mut self) -> Vectored<'_> {
        debug_assert!(self.active);
        let Self {
            iovs,
            iov_storage,
            msghdr_storage,
            len,
            ..
        } = self;
        Vectored::from_stable(FlightSource {
            iovs: &iovs[..*len],
            iov_storage,
            msghdr_storage,
        })
    }
}

impl<const IOV: usize> Drop for Flight<IOV> {
    fn drop(&mut self) {
        if self.target.is_some() {
            std::process::abort();
        }
    }
}

struct FlightSource<'a> {
    iovs: &'a [IoVec],
    iov_storage: &'a mut [IoVec],
    msghdr_storage: &'a mut MsgHdr,
}

// SAFETY: QueueState pins descriptor storage; StableBytes and Flight keep
// each pointed-to byte live and immutable through terminal completion.
unsafe impl<'a> StableVectoredSource<'a> for FlightSource<'a> {
    fn into_parts(self) -> (&'a [IoVec], &'a mut [IoVec], &'a mut MsgHdr) {
        (self.iovs, self.iov_storage, self.msghdr_storage)
    }
}
