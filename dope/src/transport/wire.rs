use crate::transport::link::Core;
use crate::{Driver, backend};

pub struct Vectored<'a> {
    pub iovs: &'a [backend::socket::IoVec],
    pub iov_storage: &'a mut [backend::socket::IoVec],
    pub msghdr_storage: &'a mut backend::socket::MsgHdr,
}

impl<'a> Vectored<'a> {
    pub(super) fn install_into_msghdr(&mut self) {
        let n = self.iovs.len();
        self.iov_storage[..n].copy_from_slice(self.iovs);
        self.msghdr_storage.set_iov(&self.iov_storage[..n]);
    }

    pub(super) fn msghdr_storage(&self) -> &backend::socket::MsgHdr {
        self.msghdr_storage
    }
}

pub enum RecvChunk<'a> {
    Borrowed(&'a [u8]),
    Owned(Vec<u8>),
}

impl<'a> RecvChunk<'a> {
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Borrowed(b) => b,
            Self::Owned(v) => v.as_slice(),
        }
    }

    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub enum Reclaim {
    OnSubmit,
    OnComplete,
}

pub trait Wire: 'static + Sized {
    type InitConfig: 'static + Clone + Default;

    const RECLAIM: Reclaim;

    /// Recv bytes reach the app untransformed, making in-kernel discard exact.
    const RAW_RECV: bool = false;

    fn new(cfg: &Self::InitConfig) -> Self;

    /// Plaintext taken but not yet turned into a socket send. The slot parks on
    /// it — no further bytes are handed over until [`Wire::flush_pending`] frees it.
    ///
    /// ```ignore
    /// fn holds_plain(&self) -> bool {
    ///     !self.pending.is_empty()
    /// }
    /// ```
    fn holds_plain(&self) -> bool {
        false
    }

    fn process_recv<'a>(&mut self, bytes: &'a [u8]) -> Option<RecvChunk<'a>>;

    fn on_recv_eof(&mut self) {}

    fn submit_send<'d>(
        &mut self,
        core: &mut Core<'d>,
        plain: &[u8],
        ud: backend::token::Token,
        driver: &'d Driver,
    ) -> usize;

    fn submit_send_vectored<'d>(
        &mut self,
        core: &mut Core<'d>,
        vectored: Vectored<'_>,
        ud: backend::token::Token,
        driver: &'d Driver,
    ) -> usize;

    fn after_send_cqe<'d>(
        &mut self,
        core: &mut Core<'d>,
        n: usize,
        ud: backend::token::Token,
        driver: &'d Driver,
    ) -> bool;

    fn flush_pending<'d>(
        &mut self,
        core: &mut Core<'d>,
        ud: backend::token::Token,
        driver: &'d Driver,
    );

    fn on_graceful_close<'d>(
        &mut self,
        _core: &mut Core<'d>,
        _ud: backend::token::Token,
        _driver: &'d Driver,
    ) {
    }
}

pub struct Identity;

impl Wire for Identity {
    type InitConfig = ();

    const RECLAIM: Reclaim = Reclaim::OnComplete;

    const RAW_RECV: bool = true;

    fn new(_: &()) -> Self {
        Identity
    }

    fn process_recv<'a>(&mut self, bytes: &'a [u8]) -> Option<RecvChunk<'a>> {
        Some(RecvChunk::Borrowed(bytes))
    }

    fn submit_send<'d>(
        &mut self,
        core: &mut Core<'d>,
        plain: &[u8],
        ud: backend::token::Token,
        driver: &'d Driver,
    ) -> usize {
        if plain.is_empty() {
            return 0;
        }
        core.submit_single(ud, plain, driver);
        plain.len()
    }

    fn submit_send_vectored<'d>(
        &mut self,
        core: &mut Core<'d>,
        vectored: Vectored<'_>,
        ud: backend::token::Token,
        driver: &'d Driver,
    ) -> usize {
        if vectored.iovs.is_empty() {
            return 0;
        }
        let consumed: usize = vectored.iovs.iter().map(|v| v.len()).sum();
        core.submit_vectored(ud, vectored, driver);
        consumed
    }

    fn after_send_cqe<'d>(
        &mut self,
        _core: &mut Core<'d>,
        _n: usize,
        _ud: backend::token::Token,
        _driver: &'d Driver,
    ) -> bool {
        false
    }

    fn flush_pending<'d>(
        &mut self,
        _core: &mut Core<'d>,
        _ud: backend::token::Token,
        _driver: &'d Driver,
    ) {
    }
}
