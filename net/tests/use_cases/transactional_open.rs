#![forbid(unsafe_code)]

use std::collections::VecDeque;

use dope_net::wire::send::{Plain, Prepared, Storage, Vectored};
use dope_net::wire::{OpenReservation, Reclaim, RuntimeLimits, Wire};
use o3::buffer::{Borrowed, Bytes};

struct Config(VecDeque<u32>);

struct Runtime {
    source: VecDeque<u32>,
    retry: Option<u32>,
}

struct TransactionalWire(u32);

struct Open<'a> {
    runtime: &'a mut Runtime,
    value: Option<u32>,
}

impl Drop for Open<'_> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            assert!(self.runtime.retry.replace(value).is_none());
        }
    }
}

impl OpenReservation<TransactionalWire> for Open<'_> {
    fn commit(mut self) -> (TransactionalWire, ()) {
        (TransactionalWire(self.value.take().unwrap()), ())
    }
}

impl Wire for TransactionalWire {
    type InitConfig = Config;
    type RuntimeContext = Runtime;
    type Open<'a> = Open<'a>;
    type Recv<'a> = Bytes<Borrowed<'a>>;
    type SendStorage = ();

    const RECLAIM: Reclaim = Reclaim::OnComplete;

    fn runtime_context(
        _: RuntimeLimits,
        config: Self::InitConfig,
    ) -> std::io::Result<Self::RuntimeContext> {
        Ok(Runtime {
            source: config.0,
            retry: None,
        })
    }

    fn prepare_open(runtime: &mut Self::RuntimeContext) -> Option<Self::Open<'_>> {
        let value = runtime
            .retry
            .take()
            .or_else(|| runtime.source.pop_front())?;
        Some(Open {
            runtime,
            value: Some(value),
        })
    }

    fn process_recv<'a>(
        &mut self,
        _: &mut Self::RuntimeContext,
        bytes: &'a [u8],
    ) -> Option<Self::Recv<'a>> {
        Some(Bytes::<Borrowed<'a>>::from(bytes))
    }

    fn prepare_send<'a>(
        &'a mut self,
        _: Storage<'a, Self::SendStorage>,
        plain: Plain<'a>,
    ) -> Prepared<'a> {
        let len = plain.len();
        Prepared::input(plain, len)
    }

    fn prepare_send_vectored<'a>(
        &'a mut self,
        _: Storage<'a, Self::SendStorage>,
        plain: Vectored<'a>,
    ) -> Prepared<'a> {
        let len = plain.bytes();
        Prepared::vectored(plain, len)
    }

    fn after_send<'a>(
        &'a mut self,
        send: Storage<'a, Self::SendStorage>,
        _: usize,
    ) -> Prepared<'a> {
        send.empty(0)
    }

    fn flush_pending<'a>(&'a mut self, send: Storage<'a, Self::SendStorage>) -> Prepared<'a> {
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
fn siege_cancellation_never_consumes_the_one_shot_open() {
    let mut runtime = runtime([73]);

    for _ in 0..65_536 {
        drop(TransactionalWire::prepare_open(&mut runtime).unwrap());
    }

    let (wire, ()) = TransactionalWire::prepare_open(&mut runtime)
        .unwrap()
        .commit();
    assert_eq!(wire.0, 73);
    assert!(TransactionalWire::prepare_open(&mut runtime).is_none());
}

#[test]
fn siege_commit_sequence_has_no_gaps_after_transient_failures() {
    let mut runtime = runtime(0..4096);
    let mut committed = Vec::new();
    let mut attempt = 0usize;

    while committed.len() != 4096 {
        let open = TransactionalWire::prepare_open(&mut runtime).unwrap();
        attempt += 1;
        if !attempt.is_multiple_of(7) {
            drop(open);
            continue;
        }
        committed.push(open.commit().0.0);
    }

    assert_eq!(committed, (0..4096).collect::<Vec<_>>());
    assert!(TransactionalWire::prepare_open(&mut runtime).is_none());
}
