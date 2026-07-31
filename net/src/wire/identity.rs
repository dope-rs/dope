use super::send::{Plain, Prepared, Sent, Storage, Vectored};
use super::{ReadyOpen, Reclaim, RecvChunk, RuntimeLimits, Wire};
use crate::{Bytes, ProvidedLease, ProvidedView};
use o3::buffer::Borrowed;
use std::io;
use std::iter::{Once, once};

pub struct Identity;

impl Wire for Identity {
    type Connection<'d> = Self;
    type ConnectionStorage = ();
    type InitConfig<'d> = ();
    type RuntimeContext<'d> = ();
    type Open<'a, 'd>
        = ReadyOpen<Self::Connection<'d>, Self::SendStorage>
    where
        'd: 'a;
    type Recv<'a> = Bytes<Borrowed<'a>>;
    type RecvBatch<'a> = Once<RecvChunk<'a, Self::Recv<'a>>>;
    type RetainedRecv<'d> = ProvidedView<'d>;
    type SendStorage = ();

    const RECLAIM: Reclaim = Reclaim::OnComplete;

    const RAW_RECV: bool = true;

    fn connection_storage(_: usize) -> io::Result<()> {
        Ok(())
    }

    fn runtime_context<'d>(
        _: RuntimeLimits,
        _: Self::InitConfig<'d>,
    ) -> io::Result<Self::RuntimeContext<'d>>
    where
        Self: 'd,
    {
        Ok(())
    }

    fn prepare_open<'a, 'd>(_: &'a mut ()) -> Option<Self::Open<'a, 'd>>
    where
        'd: 'a,
    {
        Some(ReadyOpen::new(Self, ()))
    }

    fn process_recv<'a, 'd>(
        _: &mut Self::Connection<'d>,
        _: &mut (),
        bytes: &'a mut [u8],
    ) -> Self::RecvBatch<'a> {
        once(RecvChunk::Borrowed(Bytes::<Borrowed<'a>>::from(&*bytes)))
    }

    fn process_retained_recv<'a, 'd>(
        _: &mut Self::Connection<'d>,
        _: &mut (),
        bytes: ProvidedLease<'a>,
    ) -> Option<Self::RetainedRecv<'a>> {
        let len = bytes.as_slice().len();
        let span = bytes.span(0, len)?;
        bytes.into_view(span).ok()
    }

    fn prepare_send<'a, 'd>(
        _: &'a mut Self::Connection<'d>,
        _send: Storage<'a, ()>,
        plain: Plain<'a>,
    ) -> Prepared<'a> {
        let n = plain.len();
        if n == 0 {
            Prepared::empty(0)
        } else {
            Prepared::input(plain, n)
        }
    }

    fn prepare_send_vectored<'a, 'd>(
        _: &'a mut Self::Connection<'d>,
        _send: Storage<'a, ()>,
        plain: Vectored<'a>,
    ) -> Prepared<'a> {
        if plain.is_empty() {
            return Prepared::empty(0);
        }
        let consumed = plain.bytes();
        Prepared::vectored(plain, consumed)
    }

    fn after_send<'a, 'd>(
        _: &'a mut Self::Connection<'d>,
        _send: Storage<'a, ()>,
        _sent: Sent,
    ) -> Prepared<'a> {
        Prepared::empty(0)
    }

    fn flush_pending<'a, 'd>(
        _: &'a mut Self::Connection<'d>,
        _send: Storage<'a, ()>,
    ) -> Prepared<'a> {
        Prepared::empty(0)
    }
}
