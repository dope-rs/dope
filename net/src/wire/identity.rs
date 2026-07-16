use super::send::{Plain, Prepared, Storage, Vectored};
use super::{Reclaim, RuntimeLimits, Wire};
use crate::Bytes;
use o3::buffer::Borrowed;

pub struct Identity;

impl Wire for Identity {
    type InitConfig = ();
    type RuntimeContext = ();
    type Recv<'a> = Bytes<Borrowed<'a>>;
    type SendStorage = ();

    const RECLAIM: Reclaim = Reclaim::OnComplete;

    const RAW_RECV: bool = true;

    fn runtime_context(_: RuntimeLimits) -> std::io::Result<()> {
        Ok(())
    }

    fn open(_: &(), _: &()) -> Option<(Self, ())> {
        Some((Self, ()))
    }

    fn process_recv<'a>(&mut self, _: &(), bytes: &'a [u8]) -> Option<Self::Recv<'a>> {
        Some(Bytes::<Borrowed<'a>>::from(bytes))
    }

    fn prepare_send<'a>(&'a mut self, _send: Storage<'a, ()>, plain: Plain<'a>) -> Prepared<'a> {
        let n = plain.len();
        if n == 0 {
            Prepared::empty(0)
        } else {
            Prepared::input(plain, n)
        }
    }

    fn prepare_send_vectored<'a>(
        &'a mut self,
        _send: Storage<'a, ()>,
        plain: Vectored<'a>,
    ) -> Prepared<'a> {
        if plain.is_empty() {
            return Prepared::empty(0);
        }
        let consumed = plain.bytes();
        Prepared::vectored(plain, consumed)
    }

    fn after_send<'a>(&'a mut self, _send: Storage<'a, ()>, _n: usize) -> Prepared<'a> {
        Prepared::empty(0)
    }

    fn flush_pending<'a>(&'a mut self, _send: Storage<'a, ()>) -> Prepared<'a> {
        Prepared::empty(0)
    }
}
