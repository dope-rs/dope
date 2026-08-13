use std::{
    cell::Cell,
    convert::Infallible,
    error, fmt,
    io::{Read, Write},
    net::{SocketAddr, TcpListener},
    pin::Pin,
    rc::Rc,
    task::Poll,
    thread::JoinHandle,
    time::Duration,
};

use dope::{
    core::{
        driver::{settings, storage},
        io::recv::{self, View},
    },
    manifold::timing::Balanced,
    net::{
        link::pool::transition::open,
        tcp::Tcp,
        wire::{
            self, Cursor, ErasedRecvCreditGuard, Identity, ReadyOpen, RecvChunk, RecvCredit,
            RecvCreditId, RecvCreditReceipt, RuntimeLimits, Wire, reclaim,
            send::{Buffer, Plain, Prepared, Sent, Storage, Transition, Vectored},
        },
    },
    runtime::executor::{self, Application, Executor},
};
use dope_fiber::{
    abi::Fiber,
    context,
    net::{
        Io,
        connector::{Connect, Connector, Factory, Port},
        read::Lease,
    },
};
use dope_test::fibers;
use o3::buffer::bytes::{Borrowed, Bytes, Retained};

const ID: u8 = 0;
const MAX_CONN: usize = 8;

type Conn<'scope, 'd> = Connector<'scope, 'd, ID, Tcp, Identity>;

fn copy_lease<W: Wire>(mut lease: Lease<'_, '_, W>) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(lease.remaining());
    while !lease.is_empty() {
        let chunk = lease.chunk();
        let amount = chunk.len();
        bytes.extend_from_slice(chunk);
        assert_eq!(lease.consume(amount), amount);
    }
    bytes
}

#[derive(Debug)]
struct FailingOpenError;

impl fmt::Display for FailingOpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("permanent fiber wire open failure")
    }
}

impl error::Error for FailingOpenError {}

struct FailingWire;

impl Wire for FailingWire {
    type Connection<'d, const ID: u8> = ();
    type ConnectionStorage<const ID: u8> = ();
    type InitConfig<'d, const ID: u8> = ();
    type RuntimeContext<'d, const ID: u8> = ();
    type Open<'a, 'd, const ID: u8>
        = ReadyOpen<(), ()>
    where
        'd: 'a;
    type OpenError = FailingOpenError;
    type Recv<'a> = Bytes<Borrowed<'a>>;
    type RecvBatch<'a> = std::iter::Empty<RecvChunk<'a, Self::Recv<'a>>>;
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

    fn runtime_context<'d, const ID: u8>(_: RuntimeLimits, _: ()) -> std::io::Result<()>
    where
        Self: 'd,
    {
        Ok(())
    }

    fn prepare_open<'a, 'd, const ID: u8>(
        _: &'a mut (),
    ) -> Result<Option<Self::Open<'a, 'd, ID>>, Self::OpenError>
    where
        'd: 'a,
    {
        Err(FailingOpenError)
    }

    fn process_recv<'a, 'd, const ID: u8>(
        _: &mut Self::Connection<'d, ID>,
        _: &mut Self::RuntimeContext<'d, ID>,
        _: &'a mut [u8],
        _: &wire::batch::Capacity<Self>,
    ) -> Self::RecvBatch<'a>
    where
        'd: 'a,
    {
        std::iter::empty()
    }

    fn process_retained_recv<'a, 'd, const ID: u8>(
        _: &mut Self::Connection<'d, ID>,
        _: &mut Self::RuntimeContext<'d, ID>,
        _: recv::Lease<'a>,
    ) -> Option<Self::RetainedRecv<'a>>
    where
        'd: 'a,
    {
        None
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

#[derive(Default)]
struct GatedProbe {
    attempts: Cell<usize>,
    recv_releases: Cell<usize>,
    retained: fibers::Gate,
    retired: fibers::Gate,
}

struct RecvGatedWire {
    established: bool,
    probe: Rc<GatedProbe>,
}

struct ObservedReceive;

impl Drop for RecvGatedWire {
    fn drop(&mut self) {
        self.probe.retired.hit();
    }
}

struct CreditView<'d> {
    view: View<'d>,
    credit: Option<ErasedRecvCreditGuard<'d>>,
}

impl<'d> Cursor<'d> for CreditView<'d> {
    fn chunk(&self) -> &[u8] {
        self.view.as_slice()
    }

    fn consume(&mut self, requested: usize) -> usize {
        let consumed = requested.min(self.view.len());
        self.view.advance(consumed);
        if self.view.is_empty() {
            self.credit = None;
        }
        consumed
    }

    fn remaining(&self) -> usize {
        self.view.len()
    }

    fn retain(
        &self,
        range: std::ops::Range<usize>,
        _: &o3::buffer::resident::Budget<'d>,
    ) -> Result<wire::RetainedBytes<'d>, wire::RetainError> {
        self.view
            .as_slice()
            .get(range)
            .map(Bytes::<Retained>::copy_from_slice)
            .map(wire::RetainedBytes::from_buffered)
            .ok_or(wire::RetainError::Range)
    }
}

impl wire::receive::Strategy<RecvGatedWire> for ObservedReceive {
    type Block<'a, 'd, const ID: u8>
        = Infallible
    where
        'd: 'a;
    type Transaction<'a, 'd, const ID: u8>
        = wire::receive::DirectTransaction<'a, 'd, ID, RecvGatedWire>
    where
        'd: 'a;

    const BACKPRESSURE: bool = false;

    fn reserve<'a, 'd, const ID: u8>(
        wire: &'a mut RecvGatedWire,
        send: &'a mut Buffer<1024>,
        runtime: &'a mut Rc<GatedProbe>,
    ) -> Result<Self::Transaction<'a, 'd, ID>, Self::Block<'a, 'd, ID>>
    where
        'd: 'a,
    {
        <wire::receive::Direct as wire::receive::Strategy<RecvGatedWire>>::reserve::<ID>(
            wire, send, runtime,
        )
    }

    fn cancel<'d, const ID: u8>(_: &mut Rc<GatedProbe>, _: RecvCreditId<'d, ID>)
    where
        RecvGatedWire: 'd,
    {
    }

    fn recv_released<'d, const ID: u8>(runtime: &mut Rc<GatedProbe>)
    where
        RecvGatedWire: 'd,
    {
        runtime.recv_releases.set(runtime.recv_releases.get() + 1);
    }

    fn send_released<'d, const ID: u8>(_: &mut Rc<GatedProbe>)
    where
        RecvGatedWire: 'd,
    {
    }
}

impl Wire for RecvGatedWire {
    type Connection<'d, const ID: u8> = Self;
    type ConnectionStorage<const ID: u8> = ();
    type InitConfig<'d, const ID: u8> = Rc<GatedProbe>;
    type RuntimeContext<'d, const ID: u8> = Rc<GatedProbe>;
    type Open<'a, 'd, const ID: u8>
        = ReadyOpen<Self::Connection<'d, ID>, Self::StorageBackend<'d>>
    where
        'd: 'a;
    type OpenError = Infallible;
    type Recv<'a> = Bytes<Borrowed<'a>>;
    type RecvBatch<'a> = std::option::IntoIter<RecvChunk<'a, Self::Recv<'a>>>;
    type RetainedRecv<'d> = CreditView<'d>;
    type StorageBackend<'d>
        = Buffer<1024>
    where
        Self: 'd;
    type Reclaim = reclaim::OnSubmit;
    type Receive = ObservedReceive;
    const RECV_CREDIT: bool = true;

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
        Ok(config)
    }

    fn prepare_open<'a, 'd, const ID: u8>(
        runtime: &'a mut Self::RuntimeContext<'d, ID>,
    ) -> Result<Option<Self::Open<'a, 'd, ID>>, Infallible>
    where
        'd: 'a,
    {
        Ok(Some(ReadyOpen::new(
            Self {
                established: false,
                probe: runtime.clone(),
            },
            Buffer::new(),
        )))
    }

    fn holds_plain<'d, const ID: u8>(
        _: &Self::Connection<'d, ID>,
        send: &Self::StorageBackend<'d>,
    ) -> bool {
        !send.is_empty()
    }

    fn process_recv<'a, 'd, const ID: u8>(
        wire: &mut Self::Connection<'d, ID>,
        _: &mut Self::RuntimeContext<'d, ID>,
        bytes: &'a mut [u8],
        _: &wire::batch::Capacity<Self>,
    ) -> Self::RecvBatch<'a>
    where
        'd: 'a,
    {
        if !wire.established {
            wire.established = true;
            None.into_iter()
        } else {
            Some(RecvChunk::Borrowed(Bytes::<Borrowed<'a>>::from(&*bytes))).into_iter()
        }
    }

    fn process_retained_recv<'a, 'd, const ID: u8>(
        wire: &mut Self::Connection<'d, ID>,
        _: &mut Self::RuntimeContext<'d, ID>,
        bytes: recv::Lease<'a>,
    ) -> Option<Self::RetainedRecv<'a>>
    where
        'd: 'a,
    {
        if !wire.established {
            wire.established = true;
            return None;
        }
        let retained = Some(CreditView {
            view: bytes.into_view(),
            credit: None,
        });
        if retained.is_some() {
            wire.probe.retained.hit();
        }
        retained
    }

    fn bind_recv_credit<'d, const ID: u8>(
        recv: &mut Self::RetainedRecv<'d>,
        credit: RecvCredit<'d, ID>,
    ) -> Result<RecvCreditReceipt<'d, ID>, RecvCredit<'d, ID>> {
        match credit.claim() {
            Ok(credit) => {
                let (guard, receipt) = credit.erase();
                recv.credit = Some(guard);
                Ok(receipt)
            }
            Err(credit) => Err(credit),
        }
    }

    fn prepare_send<'a, 'd, const ID: u8>(
        wire: &'a mut Self::Connection<'d, ID>,
        mut send: Storage<'a, Self::StorageBackend<'d>>,
        plain: Plain<'a>,
    ) -> Prepared<'a, Self::Reclaim> {
        wire.probe.attempts.set(wire.probe.attempts.get() + 1);
        if !wire.established {
            return send.empty();
        }
        let n = send.spare_capacity().min(plain.len());
        assert!(send.try_extend(&plain.as_slice()[..n]));
        send.buffered(n)
    }

    fn prepare_send_vectored<'a, 'd, const ID: u8>(
        wire: &'a mut Self::Connection<'d, ID>,
        mut send: Storage<'a, Self::StorageBackend<'d>>,
        plain: Vectored<'a>,
    ) -> Prepared<'a, Self::Reclaim> {
        wire.probe.attempts.set(wire.probe.attempts.get() + 1);
        if !wire.established {
            return send.empty();
        }
        let mut consumed = 0;
        for bytes in plain.iter() {
            let n = send.spare_capacity().min(bytes.len());
            assert!(send.try_extend(&bytes[..n]));
            consumed += n;
            if n != bytes.len() {
                break;
            }
        }
        send.buffered(consumed)
    }

    fn after_send<'a, 'd, const ID: u8>(
        _: &'a mut Self::Connection<'d, ID>,
        mut send: Storage<'a, Self::StorageBackend<'d>>,
        sent: Sent,
    ) -> Transition<'a, Self::Reclaim> {
        assert!(send.try_consume(sent.get()));
        Transition::unchanged(send.buffered(0))
    }

    fn flush_pending<'a, 'd, const ID: u8>(
        _: &'a mut Self::Connection<'d, ID>,
        send: Storage<'a, Self::StorageBackend<'d>>,
    ) -> Prepared<'a, Self::Reclaim> {
        send.buffered(0)
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
struct App<'d, 'scope> {
    #[pin]
    #[manifold]
    connector: Conn<'scope, 'd>,
    #[dispatcher(marker)]
    driver: ::core::marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

type FailingConn<'scope, 'd> = Connector<'scope, 'd, ID, Tcp, FailingWire>;
type FailingConnect<'scope, 'd> = Connect<'scope, 'd, Tcp, FailingWire>;

struct FailureReuse<'scope, 'd> {
    first: Option<FailingConnect<'scope, 'd>>,
    second: Option<FailingConnect<'scope, 'd>>,
    second_error: Option<std::io::Error>,
    first_started: bool,
}

impl<'scope, 'd> Fiber<'d> for FailureReuse<'scope, 'd> {
    type Output = (std::io::Error, std::io::Error);

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (self_, mut cx) = call.into_parts();
        let this = self_.get_mut();
        if !this.first_started {
            let first = this.first.as_mut().expect("first connect");
            let Some(poll) = cx.as_mut().try_poll(Pin::new(first)) else {
                return Poll::Pending;
            };
            assert!(
                poll.is_pending(),
                "first open failure must complete asynchronously"
            );
            this.first_started = true;
            return Poll::Pending;
        }

        if this.second_error.is_none() {
            let second = this.second.as_mut().expect("second connect");
            let Some(poll) = cx.as_mut().try_poll(Pin::new(second)) else {
                return Poll::Pending;
            };
            this.second_error = Some(match poll {
                Poll::Ready(Err(error)) => error,
                Poll::Ready(Ok(_)) => panic!("second failing wire unexpectedly connected"),
                Poll::Pending => panic!("second connect reused the unread failed generation"),
            });
            this.second = None;
        }

        let first = this.first.as_mut().expect("first connect");
        let Some(poll) = cx.as_mut().try_poll(Pin::new(first)) else {
            return Poll::Pending;
        };
        let first_error = match poll {
            Poll::Ready(Err(error)) => error,
            Poll::Ready(Ok(_)) => panic!("first failing wire unexpectedly connected"),
            Poll::Pending => panic!("first owned failure was lost"),
        };
        this.first = None;
        Poll::Ready((
            first_error,
            this.second_error.take().expect("second failure result"),
        ))
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
struct FailingApp<'d, 'scope> {
    #[pin]
    #[manifold]
    connector: FailingConn<'scope, 'd>,
    #[dispatcher(marker)]
    driver: ::core::marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

type GatedConn<'scope, 'd> = Connector<'scope, 'd, ID, Tcp, RecvGatedWire>;

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
struct GatedApp<'d, 'scope> {
    #[pin]
    #[manifold]
    connector: GatedConn<'scope, 'd>,
    #[dispatcher(marker)]
    driver: ::core::marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

fn connector_exec<W: Wire>(max_connections: usize) -> Executor<Factory<Tcp, W>> {
    let cfg =
        settings::Config::for_tcp_profile::<Balanced>(max_connections).expect("driver config");
    Executor::new(cfg)
        .expect("executor")
        .with_factory(Port::<Tcp, W>::factory(max_connections).expect("connector capacity"))
}

struct ForgetAfterPoll<F> {
    fiber: Option<Pin<Box<F>>>,
}

fn forget_after_poll<'d, F: Fiber<'d>>(fiber: F) -> impl Fiber<'d, Output = ()> {
    ForgetAfterPoll {
        fiber: Some(Box::pin(fiber)),
    }
}

impl<'d, F: Fiber<'d>> Fiber<'d> for ForgetAfterPoll<F> {
    type Output = ();

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (self_, mut context) = call.into_parts();
        let this = self_.get_mut();
        let fiber = this.fiber.as_mut().expect("test fiber polled once");
        let Some(poll) = context.as_mut().try_poll(fiber.as_mut()) else {
            return Poll::Pending;
        };
        assert!(
            poll.is_pending(),
            "write unexpectedly completed before reaching the port"
        );
        let fiber = this.fiber.take().expect("test fiber still present");
        std::mem::forget(fiber);
        Poll::Ready(())
    }
}

#[test]
fn duplicate_connector_route_fails_storage_construction() {
    let cfg = settings::Config::for_tcp_profile::<Balanced>(2).expect("driver config");
    let first = Port::<Tcp, Identity>::factory(1).expect("first connector capacity");
    let second = Port::<Tcp, Identity>::factory(1).expect("second connector capacity");
    let result = Executor::new(cfg)
        .expect("executor")
        .with_factory((first, second))
        .try_enter(|_| ());

    match result {
        Err(storage::PairError::Second(error)) => {
            assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
        }
        Err(storage::PairError::First(error)) => panic!("first route failed: {error}"),
        Ok(()) => panic!("duplicate connector route was accepted"),
    }
}

fn spawn_reply_server(
    response: &'static [u8],
    accept_attempts: usize,
) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind reply server");
    let addr = listener.local_addr().expect("local addr");
    let server = std::thread::spawn(move || {
        for _ in 0..accept_attempts {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("set read timeout");
            let mut req = [0u8; 64];
            match stream.read(&mut req) {
                Ok(n) if n > 0 => {
                    stream.write_all(response).expect("server write");
                    return;
                }
                _ => continue,
            }
        }
    });
    (addr, server)
}

fn spawn_gated_reply_server(response: &'static [u8]) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind reply server");
    let addr = listener.local_addr().expect("local addr");
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        std::thread::sleep(Duration::from_millis(100));
        stream.write_all(b"H").expect("server handshake");
        let mut request = [0u8; 4];
        stream.read_exact(&mut request).expect("server request");
        assert_eq!(&request, b"ping");
        stream.write_all(response).expect("server response");
    });
    (addr, server)
}

fn ping_roundtrip<'app, 'scope, 'd: 'scope + 'app, D: Application<'d>, W: Wire>(
    app: &mut executor::session::Application<'app, 'd, D>,
    io: Io<'scope, 'd, W, dope::manifold::connector::connection::Id<'d, ID>>,
) -> Vec<u8> {
    let mut io = Some(io);
    let mut io = fibers::TEST
        .drive(
            app,
            dope_gen::fiber!('_, crate = ::dope_fiber => async move {
                let mut io = io.take().expect("io owner");
                io.write_all(b"ping").await?;
                Ok::<_, std::io::Error>(io)
            }),
        )
        .expect("write request");
    let mut got = Vec::new();
    loop {
        let mut owner = Some(io);
        let (next, buf) = fibers::TEST
            .drive(
                app,
                dope_gen::fiber!('_, crate = ::dope_fiber => async move {
                    let mut io = owner.take().expect("io owner");
                    let buf = io.read().await?.map(copy_lease).unwrap_or_default();
                    Ok::<_, std::io::Error>((io, buf))
                }),
            )
            .expect("read response");
        io = next;
        if buf.is_empty() {
            break;
        }
        got.extend_from_slice(&buf);
    }
    got
}

#[test]
fn fiber_connector_roundtrip() {
    let response: &'static [u8] = b"PONG-fiber-connector-roundtrip";
    let (addr, server) = spawn_reply_server(response, 1);

    connector_exec(MAX_CONN)
        .try_enter(|mut sess| {
            let storage = sess.storage();
            let mut driver = sess.driver_access();
            let connector: Conn<'_, '_> = storage.connector(&mut driver).expect("connector");
            let duplicate = match storage.connector(&mut driver) {
                Ok(_) => panic!("connector route was rebound"),
                Err(error) => error,
            };
            assert_eq!(duplicate.kind(), std::io::ErrorKind::AlreadyExists);
            let conn = sess.storage().handle();

            let dispatcher = App {
                connector,
                driver: ::core::marker::PhantomData,
            };
            let got = sess
                .with_app(dispatcher, |mut app| {
                    let io = fibers::TEST
                        .drive(
                            &mut app,
                            dope_gen::fiber!('_, crate = ::dope_fiber => async move {
                                conn.connect(addr, Default::default()).await
                            }),
                        )
                        .expect("connect");
                    ping_roundtrip(&mut app, io)
                })
                .expect("application teardown");

            server.join().expect("server thread");
            assert_eq!(
                got, response,
                "fiber connector must deliver the server's response bytes"
            );
        })
        .expect("connector route");
}

#[test]
fn cancelled_and_leaked_writes_preserve_following_writes() {
    const DELIVERED: &[u8] = b"keptoldnew";
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind request server");
    let addr = listener.local_addr().expect("local addr");
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set read timeout");
        let mut request = [0; DELIVERED.len()];
        stream.read_exact(&mut request).expect("read writes");
        assert_eq!(&request, DELIVERED);
    });

    connector_exec(MAX_CONN)
        .try_enter(|mut sess| {
            let storage = sess.storage();
            let mut driver = sess.driver_access();
            let connector: Conn<'_, '_> = storage.connector(&mut driver).expect("connector");
            let conn = sess.storage().handle();
            let dispatcher = App {
                connector,
                driver: ::core::marker::PhantomData,
            };

            sess.with_app(dispatcher, |mut app| {
                let io = fibers::TEST
                    .drive(
                        &mut app,
                        dope_gen::fiber!('_, crate = ::dope_fiber => async move {
                            conn.connect(addr, Default::default()).await
                        }),
                    )
                    .expect("connect");
                let mut owner = Some(io);
                fibers::TEST
                    .drive(
                        &mut app,
                        dope_gen::fiber!('_, crate = ::dope_fiber => async move {
                            let mut io = owner.take().expect("io owner");
                            fibers::TEST.cancel_after_poll(io.write_all(b"discarded")).await;
                            io.write_all(b"kept").await?;
                            forget_after_poll(io.write_all(b"old")).await;
                            io.write_all(b"new").await
                        }),
                    )
                    .expect("write sequence");
            })
            .expect("application teardown");
            server.join().expect("server thread");
        })
        .expect("connector route");
}

#[test]
fn writes_after_remote_close_remain_broken_pipe() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind close server");
    let addr = listener.local_addr().expect("local addr");
    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept");
        drop(stream);
    });

    connector_exec(MAX_CONN)
        .try_enter(|mut sess| {
            let storage = sess.storage();
            let mut driver = sess.driver_access();
            let connector: Conn<'_, '_> = storage.connector(&mut driver).expect("connector");
            let conn = sess.storage().handle();
            let dispatcher = App {
                connector,
                driver: ::core::marker::PhantomData,
            };

            let errors = sess
                .with_app(dispatcher, |mut app| {
                    let io = fibers::TEST
                        .drive(
                            &mut app,
                            dope_gen::fiber!('_, crate = ::dope_fiber => async move {
                                conn.connect(addr, Default::default()).await
                            }),
                        )
                        .expect("connect");
                    let mut owner = Some(io);
                    fibers::TEST.drive(
                        &mut app,
                        dope_gen::fiber!('_, crate = ::dope_fiber => async move {
                            let mut io = owner.take().expect("io owner");
                            while io.read().await?.is_some() {}
                            let first = io.write_all(b"first").await.unwrap_err().kind();
                            let second = io.write_all(b"second").await.unwrap_err().kind();
                            Ok::<_, std::io::Error>((first, second))
                        }),
                    )
                })
                .expect("application teardown")
                .expect("closed write sequence");
            assert_eq!(
                errors,
                (
                    std::io::ErrorKind::BrokenPipe,
                    std::io::ErrorKind::BrokenPipe
                )
            );
            server.join().expect("server thread");
        })
        .expect("connector route");
}

#[test]
fn fatal_wire_open_wakes_the_exact_connect_waiter_with_its_owned_error() {
    let address: SocketAddr = "127.0.0.1:9".parse().unwrap();

    connector_exec::<FailingWire>(1)
        .try_enter(|mut sess| {
            let storage = sess.storage();
            let mut driver = sess.driver_access();
            let connector: FailingConn<'_, '_> = storage
                .connector_with_wire((), &mut driver)
                .expect("connector");
            let handle = sess.storage().handle();

            let dispatcher = FailingApp {
                connector,
                driver: ::core::marker::PhantomData,
            };
            let result = sess
                .with_app(dispatcher, |mut app| {
                    fibers::TEST.drive(
                        &mut app,
                        dope_gen::fiber!('_, crate = ::dope_fiber => async move {
                            handle.connect(address, Default::default()).await
                        }),
                    )
                })
                .expect("application teardown");
            let error = match result {
                Ok(_) => panic!("wire open failure unexpectedly connected"),
                Err(error) => error,
            };

            assert_eq!(
                error.to_string(),
                "wire open failed: permanent fiber wire open failure"
            );
            assert!(
                error
                    .get_ref()
                    .and_then(|source| source.downcast_ref::<open::Failure<FailingOpenError>>())
                    .is_some(),
                "fiber must retain the owned typed open failure as the io::Error source"
            );
        })
        .expect("connector route");
}

#[test]
fn unread_connect_failure_cannot_be_overwritten_by_slot_reuse() {
    let address: SocketAddr = "127.0.0.1:9".parse().unwrap();

    connector_exec::<FailingWire>(1)
        .try_enter(|mut sess| {
            let storage = sess.storage();
            let mut driver = sess.driver_access();
            let connector: FailingConn<'_, '_> = storage
                .connector_with_wire((), &mut driver)
                .expect("connector");
            let handle = sess.storage().handle();
            let dispatcher = FailingApp {
                connector,
                driver: ::core::marker::PhantomData,
            };

            let (first, second) = sess
                .with_app(dispatcher, |mut app| {
                    fibers::TEST.drive(
                        &mut app,
                        FailureReuse {
                            first: Some(handle.connect(address, Default::default())),
                            second: Some(handle.connect(address, Default::default())),
                            second_error: None,
                            first_started: false,
                        },
                    )
                })
                .expect("application teardown");

            assert!(
                first
                    .get_ref()
                    .and_then(|source| source.downcast_ref::<open::Failure<FailingOpenError>>())
                    .is_some(),
                "the original typed open failure must survive until its first poll"
            );
            assert_eq!(
                second.to_string(),
                "fiber::Connector: pending pool exhausted"
            );
        })
        .expect("connector route");
}

#[test]
fn connector_parks_plaintext_until_wire_receives() {
    let response: &'static [u8] = b"PONG-gated";
    let (addr, server) = spawn_gated_reply_server(response);
    let probe = Rc::new(GatedProbe::default());

    connector_exec(MAX_CONN)
        .try_enter(|mut sess| {
            let storage = sess.storage();
            let mut driver = sess.driver_access();
            let connector: GatedConn<'_, '_> = storage
                .connector_with_wire(probe.clone(), &mut driver)
                .expect("connector");
            let conn = sess.storage().handle();

            let dispatcher = GatedApp {
                connector,
                driver: ::core::marker::PhantomData,
            };
            let got = sess
                .with_app(dispatcher, |mut app| {
                    let io = fibers::TEST
                        .drive(
                            &mut app,
                            dope_gen::fiber!('_, crate = ::dope_fiber => async move {
                                conn.connect(addr, Default::default()).await
                            }),
                        )
                        .expect("connect");
                    ping_roundtrip(&mut app, io)
                })
                .expect("application teardown");

            server.join().expect("server thread");
            assert_eq!(got, response);
            assert!(
                probe.attempts.get() <= 4,
                "wire send path spun before receive"
            );
        })
        .expect("connector route");
}

#[test]
fn connector_receive_credit_resumes_deferred_multishot_buffers() {
    static RESPONSE: [u8; 32 * 1024] = [b'R'; 32 * 1024];

    let (addr, server) = spawn_gated_reply_server(&RESPONSE);
    let probe = Rc::new(GatedProbe::default());
    let cfg = settings::Config::for_tcp_profile::<Balanced>(MAX_CONN)
        .expect("driver config")
        .with_receive(settings::Receive::fixed::<64, 1024>());
    let exec = Executor::new(cfg)
        .expect("executor")
        .with_factory(Port::<Tcp, RecvGatedWire>::factory(MAX_CONN).expect("connector capacity"));

    exec.try_enter(|mut sess| {
        let storage = sess.storage();
        let mut driver = sess.driver_access();
        let connector: GatedConn<'_, '_> = storage
            .connector_with_wire(probe.clone(), &mut driver)
            .expect("connector");
        let conn = sess.storage().handle();

        let dispatcher = GatedApp {
            connector,
            driver: ::core::marker::PhantomData,
        };
        let got = sess
            .with_app(dispatcher, |mut app| {
                let io = fibers::TEST
                    .drive(
                        &mut app,
                        dope_gen::fiber!('_, crate = ::dope_fiber => async move {
                            conn.connect(addr, Default::default()).await
                        }),
                    )
                    .expect("connect");
                let mut owner = Some(io);
                let io = fibers::TEST
                    .drive(
                        &mut app,
                        dope_gen::fiber!('_, crate = ::dope_fiber => async move {
                            let mut io = owner.take().expect("io owner");
                            io.write_all(b"ping").await?;
                            Ok::<_, std::io::Error>(io)
                        }),
                    )
                    .expect("write request");

                std::thread::sleep(Duration::from_millis(100));
                let mut io = io;
                let mut got = Vec::with_capacity(RESPONSE.len());
                while got.len() < RESPONSE.len() {
                    let mut owner = Some(io);
                    let (next, bytes) = fibers::TEST
                        .drive(
                            &mut app,
                            dope_gen::fiber!('_, crate = ::dope_fiber => async move {
                                let mut io = owner.take().expect("io owner");
                                let bytes = io.read().await?.map(copy_lease).unwrap_or_default();
                                Ok::<_, std::io::Error>((io, bytes))
                            }),
                        )
                        .expect("read response");
                    assert!(
                        !bytes.is_empty(),
                        "connection closed before deferred data drained"
                    );
                    io = next;
                    got.extend_from_slice(&bytes);
                }
                got
            })
            .expect("application teardown");

        assert_eq!(got, RESPONSE);
        assert!(
            probe.recv_releases.get() > 0,
            "dropping a retained receive credit must reach the wire strategy"
        );
        server.join().expect("server thread");
    })
    .expect("connector route");
}

#[test]
fn dropping_io_releases_queued_receive_credit_before_shutdown() {
    static RESPONSE: [u8; 1024] = [b'Q'; 1024];

    let (addr, server) = spawn_gated_reply_server(&RESPONSE);
    let probe = Rc::new(GatedProbe::default());
    let cfg = settings::Config::for_tcp_profile::<Balanced>(1)
        .expect("driver config")
        .with_receive(settings::Receive::fixed::<8, 1024>());
    let exec = Executor::new(cfg)
        .expect("executor")
        .with_factory(Port::<Tcp, RecvGatedWire>::factory(1).expect("connector capacity"));

    exec.try_enter(|mut sess| {
        let storage = sess.storage();
        let mut driver = sess.driver_access();
        let connector: GatedConn<'_, '_> = storage
            .connector_with_wire(probe.clone(), &mut driver)
            .expect("connector");
        let conn = sess.storage().handle();
        let dispatcher = GatedApp {
            connector,
            driver: ::core::marker::PhantomData,
        };

        sess.with_app(dispatcher, |mut app| {
            let mut io = fibers::TEST
                .drive(
                    &mut app,
                    dope_gen::fiber!('_, crate = ::dope_fiber => async move {
                        conn.connect(addr, Default::default()).await
                    }),
                )
                .expect("connect");
            fibers::TEST
                .drive(&mut app, io.write_all(b"ping"))
                .expect("write request");
            fibers::TEST.run_until(&mut app, &probe.retained, 1);
            drop(io);
        })
        .expect("application teardown");

        assert!(
            probe.recv_releases.get() > 0,
            "detaching Io must drop its unread credited cursor"
        );
        server.join().expect("server thread");
    })
    .expect("connector route");
}

#[test]
fn returned_receive_credit_reenters_waiting_close() {
    static RESPONSE: [u8; 1024] = [b'W'; 1024];

    let (addr, server) = spawn_gated_reply_server(&RESPONSE);
    let probe = Rc::new(GatedProbe::default());
    let cfg = settings::Config::for_tcp_profile::<Balanced>(1)
        .expect("driver config")
        .with_receive(settings::Receive::fixed::<8, 1024>());
    let exec = Executor::new(cfg)
        .expect("executor")
        .with_factory(Port::<Tcp, RecvGatedWire>::factory(1).expect("connector capacity"));

    exec.try_enter(|mut sess| {
        let storage = sess.storage();
        let mut driver = sess.driver_access();
        let connector: GatedConn<'_, '_> = storage
            .connector_with_wire(probe.clone(), &mut driver)
            .expect("connector");
        let conn = sess.storage().handle();
        let dispatcher = GatedApp {
            connector,
            driver: ::core::marker::PhantomData,
        };

        sess.with_app(dispatcher, |mut app| {
            let mut io = fibers::TEST
                .drive(
                    &mut app,
                    dope_gen::fiber!('_, crate = ::dope_fiber => async move {
                        conn.connect(addr, Default::default()).await
                    }),
                )
                .expect("connect");
            fibers::TEST
                .drive(&mut app, io.write_all(b"ping"))
                .expect("write request");
            let lease = fibers::TEST
                .drive(&mut app, io.read())
                .expect("read response")
                .expect("response cursor");

            fibers::TEST.pause(&mut app, Duration::from_millis(100));
            assert_eq!(
                probe.retired.hits(),
                0,
                "the live lease must retain its exact connection generation"
            );
            drop(lease);
            fibers::TEST.run_until(&mut app, &probe.retired, 1);
            drop(io);
        })
        .expect("application teardown");

        assert!(probe.recv_releases.get() > 0);
        server.join().expect("server thread");
    })
    .expect("connector route");
}

#[test]
fn cancelled_connect_reclaims_tag_for_reuse() {
    let response: &'static [u8] = b"PONG-after-cancel";
    let (addr, server) = spawn_reply_server(response, 2);

    connector_exec(MAX_CONN)
        .try_enter(|mut sess| {
            let storage = sess.storage();
            let mut driver = sess.driver_access();
            let connector: Conn<'_, '_> = storage.connector(&mut driver).expect("connector");
            let conn = sess.storage().handle();

            let dispatcher = App {
                connector,
                driver: ::core::marker::PhantomData,
            };
            let got = sess
                .with_app(dispatcher, |mut app| {
                    fibers::TEST.drive(
                        &mut app,
                        dope_gen::fiber!('_, crate = ::dope_fiber => async move {
                            fibers::TEST
                                .cancel_after_poll(conn.connect(addr, Default::default()))
                                .await;
                        }),
                    );
                    let io = fibers::TEST
                        .drive(
                            &mut app,
                            dope_gen::fiber!('_, crate = ::dope_fiber => async move {
                                conn.connect(addr, Default::default()).await
                            }),
                        )
                        .expect("re-connect after cancel");
                    ping_roundtrip(&mut app, io)
                })
                .expect("application teardown");
            assert_eq!(got, response, "post-cancel connection must round-trip");
            server.join().expect("server join");
        })
        .expect("connector route");
}
