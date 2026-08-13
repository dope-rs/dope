#![forbid(unsafe_code)]

use std::{cell::Cell, collections::VecDeque, convert::Infallible, rc::Rc};

use dope_core::io::recv::{Lease, View};
use dope_net::wire::{
    self, OpenReservation, OpenRollback, RecvChunk, RuntimeLimits, Wire, reclaim,
    reservation::ReservedOpen,
    send::{Plain, Prepared, Sent, Storage, Transition, Vectored},
};
use o3::buffer::bytes::{Borrowed, Bytes};

struct Config(VecDeque<u32>);

struct Runtime {
    source: VecDeque<u32>,
    retry: Option<TransactionalWire>,
}

struct TransactionalWire {
    value: u32,
    drops: Option<Rc<Cell<usize>>>,
}

impl TransactionalWire {
    fn new(value: u32) -> Self {
        Self { value, drops: None }
    }

    fn tracked(value: u32, drops: Rc<Cell<usize>>) -> Self {
        Self {
            value,
            drops: Some(drops),
        }
    }
}

impl Drop for TransactionalWire {
    fn drop(&mut self) {
        if let Some(drops) = &self.drops {
            drops.set(drops.get() + 1);
        }
    }
}

impl OpenRollback<TransactionalWire, ()> for Runtime {
    fn rollback_open(&mut self, open: (TransactionalWire, ())) {
        let (wire, ()) = open;
        self.retry = Some(wire);
    }
}

impl Wire for TransactionalWire {
    type Connection<'d, const ID: u8> = Self;
    type ConnectionStorage<const ID: u8> = ();
    type InitConfig<'d, const ID: u8> = Config;
    type RuntimeContext<'d, const ID: u8> = Runtime;
    type Open<'a, 'd, const ID: u8>
        = ReservedOpen<
        'a,
        Self::Connection<'d, ID>,
        Self::StorageBackend<'d>,
        Self::RuntimeContext<'d, ID>,
    >
    where
        'd: 'a;
    type OpenError = Infallible;
    type Recv<'a> = Bytes<Borrowed<'a>>;
    type RecvBatch<'a> = std::iter::Once<RecvChunk<'a, Self::Recv<'a>>>;
    type RetainedRecv<'d> = View<'d>;
    type StorageBackend<'d>
        = ()
    where
        Self: 'd;
    type Reclaim = reclaim::OnComplete;
    type Receive = wire::receive::Direct;

    fn connection_storage<const ID: u8>(_: usize) -> std::io::Result<()> {
        Ok(())
    }

    fn runtime_context<'d, const ID: u8>(
        _: RuntimeLimits,
        config: Self::InitConfig<'d, ID>,
    ) -> std::io::Result<Self::RuntimeContext<'d, ID>>
    where
        Self: 'd,
    {
        Ok(Runtime {
            source: config.0,
            retry: None,
        })
    }

    fn prepare_open<'a, 'd, const ID: u8>(
        runtime: &'a mut Self::RuntimeContext<'d, ID>,
    ) -> Result<Option<Self::Open<'a, 'd, ID>>, Infallible>
    where
        'd: 'a,
    {
        let Some(wire) = runtime
            .retry
            .take()
            .or_else(|| runtime.source.pop_front().map(TransactionalWire::new))
        else {
            return Ok(None);
        };
        Ok(Some(ReservedOpen::new(runtime, wire, ())))
    }

    fn process_recv<'a, 'd, const ID: u8>(
        _: &mut Self::Connection<'d, ID>,
        _: &mut Self::RuntimeContext<'d, ID>,
        bytes: &'a mut [u8],
        _: &wire::batch::Capacity<Self>,
    ) -> Self::RecvBatch<'a>
    where
        'd: 'a,
    {
        std::iter::once(RecvChunk::Borrowed(Bytes::<Borrowed<'a>>::from(&*bytes)))
    }

    fn process_retained_recv<'a, 'd, const ID: u8>(
        _: &mut Self::Connection<'d, ID>,
        _: &mut Self::RuntimeContext<'d, ID>,
        bytes: Lease<'a>,
    ) -> Option<Self::RetainedRecv<'a>>
    where
        'd: 'a,
    {
        Some(bytes.into_view())
    }

    fn prepare_send<'a, 'd, const ID: u8>(
        _: &'a mut Self::Connection<'d, ID>,
        _: Storage<'a, Self::StorageBackend<'d>>,
        plain: Plain<'a>,
    ) -> Prepared<'a, Self::Reclaim> {
        Prepared::input(plain)
    }

    fn prepare_send_vectored<'a, 'd, const ID: u8>(
        _: &'a mut Self::Connection<'d, ID>,
        _: Storage<'a, Self::StorageBackend<'d>>,
        plain: Vectored<'a>,
    ) -> Prepared<'a, Self::Reclaim> {
        Prepared::vectored(plain)
    }

    fn after_send<'a, 'd, const ID: u8>(
        _: &'a mut Self::Connection<'d, ID>,
        send: Storage<'a, Self::StorageBackend<'d>>,
        _: Sent,
    ) -> Transition<'a, Self::Reclaim> {
        Transition::completed(send)
    }

    fn flush_pending<'a, 'd, const ID: u8>(
        _: &'a mut Self::Connection<'d, ID>,
        send: Storage<'a, Self::StorageBackend<'d>>,
    ) -> Prepared<'a, Self::Reclaim> {
        send.empty()
    }
}

fn runtime(values: impl IntoIterator<Item = u32>) -> Runtime {
    TransactionalWire::runtime_context::<0>(
        RuntimeLimits::new(4096, 0, 4096),
        Config(values.into_iter().collect()),
    )
    .unwrap()
}

#[test]
fn transactional_open_adds_only_the_runtime_reference() {
    type Open<'a> = ReservedOpen<'a, TransactionalWire, (), Runtime>;
    type Value = (TransactionalWire, ());
    type Expected<'a> = (Value, &'a mut Runtime);

    assert_eq!(size_of::<Open<'_>>(), size_of::<Expected<'_>>());
    assert_eq!(align_of::<Open<'_>>(), align_of::<Expected<'_>>());
}

#[test]
fn transactional_open_moves_drop_state_exactly_once() {
    let drops = Rc::new(Cell::new(0));
    let mut runtime = runtime([]);
    runtime.retry = Some(TransactionalWire::tracked(73, drops.clone()));

    drop(
        TransactionalWire::prepare_open::<0>(&mut runtime)
            .unwrap()
            .unwrap(),
    );
    assert_eq!(drops.get(), 0);

    let (wire, ()) = TransactionalWire::prepare_open::<0>(&mut runtime)
        .unwrap()
        .unwrap()
        .commit();
    assert_eq!(drops.get(), 0);

    drop(wire);
    assert_eq!(drops.get(), 1);
}

#[test]
fn siege_cancellation_never_consumes_the_one_shot_open() {
    let mut runtime = runtime([73]);

    for _ in 0..65_536 {
        drop(
            TransactionalWire::prepare_open::<0>(&mut runtime)
                .unwrap()
                .unwrap(),
        );
    }

    let (wire, ()) = TransactionalWire::prepare_open::<0>(&mut runtime)
        .unwrap()
        .unwrap()
        .commit();
    assert_eq!(wire.value, 73);
    assert!(
        TransactionalWire::prepare_open::<0>(&mut runtime)
            .unwrap()
            .is_none()
    );
}

#[test]
fn siege_commit_sequence_has_no_gaps_after_transient_failures() {
    let mut runtime = runtime(0..4096);
    let mut committed = Vec::new();
    let mut attempt = 0usize;

    while committed.len() != 4096 {
        let open = TransactionalWire::prepare_open::<0>(&mut runtime)
            .unwrap()
            .unwrap();
        attempt += 1;
        if !attempt.is_multiple_of(7) {
            drop(open);
            continue;
        }
        let (wire, ()) = open.commit();
        committed.push(wire.value);
    }

    assert_eq!(committed, (0..4096).collect::<Vec<_>>());
    assert!(
        TransactionalWire::prepare_open::<0>(&mut runtime)
            .unwrap()
            .is_none()
    );
}
