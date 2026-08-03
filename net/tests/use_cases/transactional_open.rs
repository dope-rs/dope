#![forbid(unsafe_code)]

use std::cell::Cell;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::rc::Rc;

use dope_core::io::recv::{Lease, View};
use dope_net::wire::reservation::ReservedOpen;
use dope_net::wire::send::{Plain, Prepared, Sent, Storage, Vectored};
use dope_net::wire::{OpenReservation, OpenRollback, Reclaim, RecvChunk, RuntimeLimits, Wire};
use o3::buffer::{Borrowed, Bytes};

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
    type Connection<'d> = Self;
    type ConnectionStorage = ();
    type InitConfig<'d> = Config;
    type RuntimeContext<'d> = Runtime;
    type Open<'a, 'd>
        = ReservedOpen<'a, Self::Connection<'d>, Self::SendStorage, Self::RuntimeContext<'d>>
    where
        'd: 'a;
    type OpenError = Infallible;
    type Recv<'a> = Bytes<Borrowed<'a>>;
    type RecvBatch<'a> = std::iter::Once<RecvChunk<'a, Self::Recv<'a>>>;
    type RetainedRecv<'d> = View<'d>;
    type SendStorage = ();

    const RECLAIM: Reclaim = Reclaim::OnComplete;

    fn connection_storage(_: usize) -> std::io::Result<()> {
        Ok(())
    }

    fn runtime_context<'d>(
        _: RuntimeLimits,
        config: Self::InitConfig<'d>,
    ) -> std::io::Result<Self::RuntimeContext<'d>>
    where
        Self: 'd,
    {
        Ok(Runtime {
            source: config.0,
            retry: None,
        })
    }

    fn prepare_open<'a, 'd>(
        runtime: &'a mut Self::RuntimeContext<'d>,
    ) -> Result<Option<Self::Open<'a, 'd>>, Infallible>
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

    fn process_recv<'a, 'd>(
        _: &mut Self::Connection<'d>,
        _: &mut Self::RuntimeContext<'d>,
        bytes: &'a mut [u8],
    ) -> Self::RecvBatch<'a> {
        std::iter::once(RecvChunk::Borrowed(Bytes::<Borrowed<'a>>::from(&*bytes)))
    }

    fn process_retained_recv<'a, 'd>(
        _: &mut Self::Connection<'d>,
        _: &mut Self::RuntimeContext<'d>,
        bytes: Lease<'a>,
    ) -> Option<Self::RetainedRecv<'a>> {
        let span = bytes.span(0, bytes.as_slice().len())?;
        bytes.into_view(span).ok()
    }

    fn prepare_send<'a, 'd>(
        _: &'a mut Self::Connection<'d>,
        _: Storage<'a, Self::SendStorage>,
        plain: Plain<'a>,
    ) -> Prepared<'a> {
        let len = plain.len();
        Prepared::input(plain, len)
    }

    fn prepare_send_vectored<'a, 'd>(
        _: &'a mut Self::Connection<'d>,
        _: Storage<'a, Self::SendStorage>,
        plain: Vectored<'a>,
    ) -> Prepared<'a> {
        let len = plain.bytes();
        Prepared::vectored(plain, len)
    }

    fn after_send<'a, 'd>(
        _: &'a mut Self::Connection<'d>,
        send: Storage<'a, Self::SendStorage>,
        _: Sent,
    ) -> Prepared<'a> {
        send.empty(0)
    }

    fn flush_pending<'a, 'd>(
        _: &'a mut Self::Connection<'d>,
        send: Storage<'a, Self::SendStorage>,
    ) -> Prepared<'a> {
        send.empty(0)
    }
}

fn runtime(values: impl IntoIterator<Item = u32>) -> Runtime {
    TransactionalWire::runtime_context(
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
        TransactionalWire::prepare_open(&mut runtime)
            .unwrap()
            .unwrap(),
    );
    assert_eq!(drops.get(), 0);

    let (wire, ()) = TransactionalWire::prepare_open(&mut runtime)
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
            TransactionalWire::prepare_open(&mut runtime)
                .unwrap()
                .unwrap(),
        );
    }

    let (wire, ()) = TransactionalWire::prepare_open(&mut runtime)
        .unwrap()
        .unwrap()
        .commit();
    assert_eq!(wire.value, 73);
    assert!(
        TransactionalWire::prepare_open(&mut runtime)
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
        let open = TransactionalWire::prepare_open(&mut runtime)
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
        TransactionalWire::prepare_open(&mut runtime)
            .unwrap()
            .is_none()
    );
}
