use std::{convert, io, iter, mem};

use dope_core::io::recv;
use o3::buffer::bytes;

use crate::wire::{self, batch, receive, reclaim, send};

pub struct Identity;

const _: () = assert!(
    mem::size_of::<Result<Option<wire::ReadyOpen<Identity, ()>>, convert::Infallible>>()
        == mem::size_of::<Option<wire::ReadyOpen<Identity, ()>>>()
);

impl wire::Wire for Identity {
    type Connection<'d, const ID: u8> = Self;
    type ConnectionStorage<const ID: u8> = ();
    type InitConfig<'d, const ID: u8> = ();
    type RuntimeContext<'d, const ID: u8> = ();
    type Open<'a, 'd, const ID: u8>
        = wire::ReadyOpen<Self::Connection<'d, ID>, Self::StorageBackend<'d>>
    where
        'd: 'a;
    type OpenError = convert::Infallible;
    type Recv<'a> = bytes::Bytes<bytes::Borrowed<'a>>;
    type RecvBatch<'a> = iter::Once<wire::RecvChunk<'a, Self::Recv<'a>>>;
    type RetainedRecv<'d> = recv::Shared<'d>;
    type StorageBackend<'d> = ();
    type Reclaim = reclaim::OnComplete;
    type Receive = receive::Direct;

    const RAW_RECV: bool = true;

    fn connection_storage<const ID: u8>(_: usize) -> io::Result<()> {
        Ok(())
    }

    fn runtime_context<'d, const ID: u8>(
        _: wire::RuntimeLimits,
        _: Self::InitConfig<'d, ID>,
    ) -> io::Result<Self::RuntimeContext<'d, ID>>
    where
        Self: 'd,
    {
        Ok(())
    }

    fn prepare_open<'a, 'd, const ID: u8>(
        _: &'a mut (),
    ) -> Result<Option<Self::Open<'a, 'd, ID>>, convert::Infallible>
    where
        'd: 'a,
    {
        Ok(Some(wire::ReadyOpen::new(Self, ())))
    }

    fn process_recv<'a, 'd, const ID: u8>(
        _: &mut Self::Connection<'d, ID>,
        _: &mut (),
        bytes: &'a mut [u8],
        _: &batch::Capacity<Self>,
    ) -> Self::RecvBatch<'a>
    where
        'd: 'a,
    {
        use std::iter::once;

        use crate::wire::RecvChunk;
        once(RecvChunk::Borrowed(
            bytes::Bytes::<bytes::Borrowed<'a>>::from(&*bytes),
        ))
    }

    fn process_retained_recv<'a, 'd, const ID: u8>(
        _: &mut Self::Connection<'d, ID>,
        _: &mut (),
        bytes: recv::Lease<'a>,
    ) -> Option<Self::RetainedRecv<'a>>
    where
        'd: 'a,
    {
        Some(bytes.into_shared())
    }

    fn prepare_send<'a, 'd, const ID: u8>(
        _: &'a mut Self::Connection<'d, ID>,
        send: send::Storage<'a, ()>,
        plain: send::Plain<'a>,
    ) -> send::Prepared<'a, Self::Reclaim> {
        if plain.is_empty() {
            send.empty()
        } else {
            send::Prepared::input(plain)
        }
    }

    fn prepare_send_vectored<'a, 'd, const ID: u8>(
        _: &'a mut Self::Connection<'d, ID>,
        send: send::Storage<'a, ()>,
        plain: send::Vectored<'a>,
    ) -> send::Prepared<'a, Self::Reclaim> {
        if plain.is_empty() {
            return send.empty();
        }
        send::Prepared::vectored(plain)
    }

    fn after_send<'a, 'd, const ID: u8>(
        _: &'a mut Self::Connection<'d, ID>,
        send: send::Storage<'a, ()>,
        _sent: send::Sent,
    ) -> send::Transition<'a, Self::Reclaim> {
        send::Transition::completed(send)
    }

    fn flush_pending<'a, 'd, const ID: u8>(
        _: &'a mut Self::Connection<'d, ID>,
        send: send::Storage<'a, ()>,
    ) -> send::Prepared<'a, Self::Reclaim> {
        send.empty()
    }
}
