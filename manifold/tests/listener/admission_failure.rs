use std::{cell::Cell, error::Error, fmt, pin::Pin, rc::Rc};

use dope_core::{
    driver::retained::Context,
    io::recv::{Lease, Shared},
};
use dope_manifold::{
    Bundle, Outcome,
    listener::{connection, handler::Application},
    timing::Throughput,
};
use dope_net::{
    tcp::Tcp,
    wire::{
        self, Identity, ReadyOpen, RecvChunk, RuntimeLimits, Wire, reclaim,
        send::{Plain, Prepared, Sent, Storage, Transition, Vectored},
    },
};
use dope_test::{fibers::Gate, peer::Peer, scenario::scenarios::Listener};
use o3::buffer::bytes::{Borrowed, Bytes, Retainable};

#[derive(Debug)]
struct FirstOpenFailure;

impl fmt::Display for FirstOpenFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("first listener wire open fails")
    }
}

impl Error for FirstOpenFailure {}

struct FailFirstWire;

impl Wire for FailFirstWire {
    type Connection<'d, const ID: u8> = Identity;
    type ConnectionStorage<const ID: u8> = ();
    type InitConfig<'d, const ID: u8> = ();
    type RuntimeContext<'d, const ID: u8> = Cell<bool>;
    type Open<'a, 'd, const ID: u8>
        = ReadyOpen<Self::Connection<'d, ID>, Self::StorageBackend<'d>>
    where
        'd: 'a;
    type OpenError = FirstOpenFailure;
    type Recv<'a> = Bytes<Borrowed<'a>>;
    type RecvBatch<'a> = std::iter::Once<RecvChunk<'a, Self::Recv<'a>>>;
    type RetainedRecv<'d> = Shared<'d>;
    type StorageBackend<'d>
        = ()
    where
        Self: 'd;
    type Reclaim = reclaim::OnComplete;
    type Receive = wire::receive::Direct;

    fn connection_storage<const ID: u8>(_: usize) -> std::io::Result<()> {
        Ok(())
    }

    fn runtime_context<'d, const ID: u8>(_: RuntimeLimits, _: ()) -> std::io::Result<Cell<bool>>
    where
        Self: 'd,
    {
        Ok(Cell::new(true))
    }

    fn prepare_open<'a, 'd, const ID: u8>(
        runtime: &'a mut Cell<bool>,
    ) -> Result<Option<Self::Open<'a, 'd, ID>>, FirstOpenFailure>
    where
        'd: 'a,
    {
        if runtime.replace(false) {
            return Err(FirstOpenFailure);
        }
        Ok(Some(ReadyOpen::new(Identity, ())))
    }

    fn process_recv<'a, 'd, const ID: u8>(
        wire: &mut Self::Connection<'d, ID>,
        _: &mut Cell<bool>,
        bytes: &'a mut [u8],
        _: &wire::batch::Capacity<Self>,
    ) -> Self::RecvBatch<'a>
    where
        'd: 'a,
    {
        let capacity = wire::batch::Capacity::<Identity>::full();
        <Identity as Wire>::process_recv::<ID>(wire, &mut (), bytes, &capacity)
    }

    fn process_retained_recv<'a, 'd, const ID: u8>(
        wire: &mut Self::Connection<'d, ID>,
        _: &mut Cell<bool>,
        bytes: Lease<'a>,
    ) -> Option<Self::RetainedRecv<'a>>
    where
        'd: 'a,
    {
        <Identity as Wire>::process_retained_recv::<ID>(wire, &mut (), bytes)
    }

    fn prepare_send<'a, 'd, const ID: u8>(
        wire: &'a mut Self::Connection<'d, ID>,
        send: Storage<'a, ()>,
        plain: Plain<'a>,
    ) -> Prepared<'a, Self::Reclaim> {
        <Identity as Wire>::prepare_send::<ID>(wire, send, plain)
    }

    fn prepare_send_vectored<'a, 'd, const ID: u8>(
        wire: &'a mut Self::Connection<'d, ID>,
        send: Storage<'a, ()>,
        plain: Vectored<'a>,
    ) -> Prepared<'a, Self::Reclaim> {
        <Identity as Wire>::prepare_send_vectored::<ID>(wire, send, plain)
    }

    fn after_send<'a, 'd, const ID: u8>(
        wire: &'a mut Self::Connection<'d, ID>,
        send: Storage<'a, ()>,
        sent: Sent,
    ) -> Transition<'a, Self::Reclaim> {
        <Identity as Wire>::after_send::<ID>(wire, send, sent)
    }

    fn flush_pending<'a, 'd, const ID: u8>(
        wire: &'a mut Self::Connection<'d, ID>,
        send: Storage<'a, ()>,
    ) -> Prepared<'a, Self::Reclaim> {
        <Identity as Wire>::flush_pending::<ID>(wire, send)
    }
}

struct FailFirstApp {
    accepted: Rc<Cell<u32>>,
    gate: Gate,
}

impl<'d, const ID: u8> Application<'d, ID> for FailFirstApp {
    type Conn = ();
    type Wire = FailFirstWire;
    type Input = dope_manifold::receive::Borrowed;

    fn deadline(self: Pin<&Self>) -> Option<std::time::Instant> {
        None
    }

    fn accept(
        self: Pin<&mut Self>,
        _connection: connection::Ctx<'_, 'd, ID, FailFirstWire, ()>,
        _driver: &mut Context<'_, '_, 'd>,
    ) -> Outcome {
        let this = self.get_mut();
        this.accepted.set(this.accepted.get() + 1);
        this.gate.hit();
        Outcome::CloseAfter
    }
}

impl<'d, const ID: u8> dope_manifold::listener::handler::BorrowedApplication<'d, ID>
    for FailFirstApp
{
    fn chunk<R: Retainable>(
        self: Pin<&mut Self>,
        _connection: connection::Ctx<'_, 'd, ID, FailFirstWire, ()>,
        _chunk: R,
        _driver: &mut Context<'_, '_, 'd>,
    ) -> Outcome {
        let _ = self;
        Outcome::Ok
    }
}

#[test]
fn failed_wire_open_does_not_consume_per_ip_admission() {
    let accepted = Rc::new(Cell::new(0));
    let gate = Gate::new();
    let transport = dope_net::tcp::ListenerConfig {
        per_ip_limit: Some(1),
        ..Default::default()
    };
    Listener::new(8, transport).run::<0, _, Bundle<Tcp, FailFirstWire, Throughput>, _>(
        FailFirstApp {
            accepted: accepted.clone(),
            gate: gate.clone(),
        },
        |case| {
            let first = Peer::at(case.addr()).connect();
            let second = Peer::at(case.addr()).connect();
            case.until(&gate, 1);
            drop(first);
            drop(second);

            assert_eq!(accepted.get(), 1);
        },
    );
}
