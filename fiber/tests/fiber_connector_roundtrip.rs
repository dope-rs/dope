use dope_test as common;

use std::cell::Cell;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::thread::JoinHandle;
use std::time::Duration;

extern crate dope;
use dope::io::provided::{ProvidedLease, ProvidedView};
use dope::runtime::dispatcher::Dispatcher;
use dope::runtime::executor::{Executor, Session};
use dope::runtime::profile::Balanced;
use dope_fiber::abi::Fiber;
use dope_fiber::io::Io;
use dope_fiber::net::connector::{Connector, ConnectorPort, ConnectorPortFactory};
use dope_net::tcp::Tcp;
use dope_net::wire::identity::Identity;
use dope_net::wire::send::{Plain, Prepared, SendBuf, Sent, Storage, Vectored};
use dope_net::wire::{
    ReadyOpen, Reclaim, RecvChunk, RecvCredit, RecvCreditGuard, RecvCursor, RecvTarget,
    RuntimeLimits, Wire,
};
use o3::buffer::{Borrowed, Bytes};
use o3::cell::BrandCell;

use common::drive;

const ID: u8 = 0;
const MAX_CONN: usize = 8;

type Conn<'scope, 'd> = Connector<'scope, 'd, ID, Tcp, Identity>;

struct RecvGatedWire {
    established: bool,
    attempts: Rc<Cell<usize>>,
}

struct CreditView<'d> {
    view: ProvidedView<'d>,
    credit: Option<RecvCreditGuard<'d>>,
}

impl RecvCursor for CreditView<'_> {
    fn remaining(&self) -> usize {
        self.view.len()
    }

    fn read_into(&mut self, target: &mut RecvTarget<'_>) {
        let count = target.write_prefix(self.view.as_slice());
        self.view.advance(count);
        if self.view.is_empty() {
            self.credit = None;
        }
    }
}

impl Wire for RecvGatedWire {
    type Connection<'d> = Self;
    type ConnectionStorage = ();
    type InitConfig<'d> = Rc<Cell<usize>>;
    type RuntimeContext<'d> = Rc<Cell<usize>>;
    type Open<'a, 'd>
        = ReadyOpen<Self::Connection<'d>, Self::SendStorage>
    where
        'd: 'a;
    type Recv<'a> = Bytes<Borrowed<'a>>;
    type RecvBatch<'a> = std::option::IntoIter<RecvChunk<'a, Self::Recv<'a>>>;
    type RetainedRecv<'d> = CreditView<'d>;
    type SendStorage = SendBuf<1024>;

    const RECLAIM: Reclaim = Reclaim::OnSubmit;
    const RECV_CREDIT: bool = true;

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
        Ok(config)
    }

    fn prepare_open<'a, 'd>(runtime: &'a mut Self::RuntimeContext<'d>) -> Option<Self::Open<'a, 'd>>
    where
        'd: 'a,
    {
        Some(ReadyOpen::new(
            Self {
                established: false,
                attempts: runtime.clone(),
            },
            SendBuf::new(),
        ))
    }

    fn holds_plain<'d>(_: &Self::Connection<'d>, send: &Self::SendStorage) -> bool {
        !send.is_empty()
    }

    fn process_recv<'a, 'd>(
        wire: &mut Self::Connection<'d>,
        _: &mut Self::RuntimeContext<'d>,
        bytes: &'a mut [u8],
    ) -> Self::RecvBatch<'a> {
        if !wire.established {
            wire.established = true;
            None.into_iter()
        } else {
            Some(RecvChunk::Borrowed(Bytes::<Borrowed<'a>>::from(&*bytes))).into_iter()
        }
    }

    fn process_retained_recv<'a, 'd>(
        wire: &mut Self::Connection<'d>,
        _: &mut Self::RuntimeContext<'d>,
        bytes: ProvidedLease<'a>,
    ) -> Option<Self::RetainedRecv<'a>> {
        if !wire.established {
            wire.established = true;
            return None;
        }
        let span = bytes.span(0, bytes.as_slice().len())?;
        bytes
            .into_view(span)
            .ok()
            .map(|view| CreditView { view, credit: None })
    }

    fn bind_recv_credit<'d>(
        recv: &mut Self::RetainedRecv<'d>,
        credit: RecvCredit<'d>,
    ) -> Result<(), RecvCredit<'d>> {
        match credit.claim() {
            Ok(credit) => {
                recv.credit = Some(credit);
                Ok(())
            }
            Err(credit) => Err(credit),
        }
    }

    fn prepare_send<'a, 'd>(
        wire: &'a mut Self::Connection<'d>,
        mut send: Storage<'a, Self::SendStorage>,
        plain: Plain<'a>,
    ) -> Prepared<'a> {
        wire.attempts.set(wire.attempts.get() + 1);
        if !wire.established {
            return send.empty(0);
        }
        let n = send.spare_capacity().min(plain.len());
        assert!(send.try_extend_from_slice(&plain.as_slice()[..n]));
        send.buffered(n)
    }

    fn prepare_send_vectored<'a, 'd>(
        wire: &'a mut Self::Connection<'d>,
        mut send: Storage<'a, Self::SendStorage>,
        plain: Vectored<'a>,
    ) -> Prepared<'a> {
        wire.attempts.set(wire.attempts.get() + 1);
        if !wire.established {
            return send.empty(0);
        }
        let mut consumed = 0;
        for bytes in plain.iter() {
            let n = send.spare_capacity().min(bytes.len());
            assert!(send.try_extend_from_slice(&bytes[..n]));
            consumed += n;
            if n != bytes.len() {
                break;
            }
        }
        send.buffered(consumed)
    }

    fn after_send<'a, 'd>(
        _: &'a mut Self::Connection<'d>,
        mut send: Storage<'a, Self::SendStorage>,
        sent: Sent,
    ) -> Prepared<'a> {
        assert!(send.try_consume(sent.get()));
        send.buffered(0)
    }

    fn flush_pending<'a, 'd>(
        _: &'a mut Self::Connection<'d>,
        send: Storage<'a, Self::SendStorage>,
    ) -> Prepared<'a> {
        send.buffered(0)
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct App<'d, 'scope> {
    #[pin]
    #[manifold]
    connector: Conn<'scope, 'd>,
}

type GatedConn<'scope, 'd> = Connector<'scope, 'd, ID, Tcp, RecvGatedWire>;

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct GatedApp<'d, 'scope> {
    #[pin]
    #[manifold]
    connector: GatedConn<'scope, 'd>,
}

struct PollOnce<F> {
    fiber: Option<F>,
}

impl<F> PollOnce<F> {
    fn new(fiber: F) -> Self {
        Self { fiber: Some(fiber) }
    }
}

impl<'d, F> Fiber<'d> for PollOnce<F>
where
    F: Fiber<'d>,
{
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: std::pin::Pin<&mut dope_fiber::raw::task::Context<'_, 'd>>,
    ) -> std::task::Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        if let Some(fiber) = &mut this.fiber {
            let _ = Fiber::poll(unsafe { std::pin::Pin::new_unchecked(fiber) }, cx);
        }
        this.fiber = None;
        std::task::Poll::Ready(())
    }
}

fn connector_exec<W: Wire>(max_connections: usize) -> Executor<ConnectorPortFactory<Tcp, W>> {
    let cfg = dope::driver::Config::for_tcp_profile::<Balanced>(max_connections);
    Executor::new(cfg).expect("executor").with_storage_factory(
        ConnectorPort::<Tcp, W>::factory(max_connections).expect("connector capacity"),
    )
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

fn ping_roundtrip<'scope, 'd, D: Dispatcher<'d>, W: Wire>(
    sess: &mut Session<'scope, 'd, ConnectorPort<'d, Tcp, W>>,
    app: Pin<&BrandCell<'d, D>>,
    io: Io<'scope, 'd, W>,
) -> Vec<u8> {
    let mut io = Some(io);
    let mut io = drive(
        sess,
        app,
        dope_gen::fiber!('_ => async move {
            let mut io = io.take().expect("io owner");
            io.write_all(b"ping").await?;
            Ok::<_, std::io::Error>(io)
        }),
    )
    .expect("write request");
    let mut got = Vec::new();
    loop {
        let mut owner = Some(io);
        let (next, buf) = drive(
            sess,
            app,
            dope_gen::fiber!('_ => async move {
                let mut io = owner.take().expect("io owner");
                let (result, buf) = io.read(Vec::with_capacity(16)).await;
                result?;
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

    connector_exec(MAX_CONN).enter(|mut sess| {
        let (storage, mut driver) = sess.storage_and_driver();
        let connector: Conn<'_, '_> = storage.connector(&mut driver).expect("connector");
        let app = pin!(BrandCell::new(App { connector }));
        let conn = sess.storage().handle();

        let io = drive(
            &mut sess,
            app.as_ref(),
            dope_gen::fiber!('_ => async move { conn.connect(addr, Default::default()).await }),
        )
        .expect("connect");
        let got = ping_roundtrip(&mut sess, app.as_ref(), io);

        server.join().expect("server thread");
        assert_eq!(
            got, response,
            "fiber connector must deliver the server's response bytes"
        );
    });
}

#[test]
fn connector_parks_plaintext_until_wire_receives() {
    let response: &'static [u8] = b"PONG-gated";
    let (addr, server) = spawn_gated_reply_server(response);
    let attempts = Rc::new(Cell::new(0));

    connector_exec(MAX_CONN).enter(|mut sess| {
        let (storage, mut driver) = sess.storage_and_driver();
        let connector: GatedConn<'_, '_> = storage
            .connector_with_wire(attempts.clone(), &mut driver)
            .expect("connector");
        let app = pin!(BrandCell::new(GatedApp { connector }));
        let conn = sess.storage().handle();

        let io = drive(
            &mut sess,
            app.as_ref(),
            dope_gen::fiber!('_ => async move { conn.connect(addr, Default::default()).await }),
        )
        .expect("connect");
        let got = ping_roundtrip(&mut sess, app.as_ref(), io);

        server.join().expect("server thread");
        assert_eq!(got, response);
        assert!(attempts.get() <= 4, "wire send path spun before receive");
    });
}

#[test]
fn connector_receive_credit_resumes_deferred_multishot_buffers() {
    static RESPONSE: [u8; 32 * 1024] = [b'R'; 32 * 1024];

    let (addr, server) = spawn_gated_reply_server(&RESPONSE);
    let attempts = Rc::new(Cell::new(0));
    let cfg = dope::driver::Config::for_tcp_profile::<Balanced>(MAX_CONN).with_provided(1024, 64);
    let exec = Executor::new(cfg).expect("executor").with_storage_factory(
        ConnectorPort::<Tcp, RecvGatedWire>::factory(MAX_CONN).expect("connector capacity"),
    );

    exec.enter(|mut sess| {
        let (storage, mut driver) = sess.storage_and_driver();
        let connector: GatedConn<'_, '_> = storage
            .connector_with_wire(attempts, &mut driver)
            .expect("connector");
        let app = pin!(BrandCell::new(GatedApp { connector }));
        let conn = sess.storage().handle();

        let io = drive(
            &mut sess,
            app.as_ref(),
            dope_gen::fiber!('_ => async move { conn.connect(addr, Default::default()).await }),
        )
        .expect("connect");
        let mut owner = Some(io);
        let io = drive(
            &mut sess,
            app.as_ref(),
            dope_gen::fiber!('_ => async move {
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
            let (next, bytes) = drive(
                &mut sess,
                app.as_ref(),
                dope_gen::fiber!('_ => async move {
                    let mut io = owner.take().expect("io owner");
                    let (read, bytes) = io.read(Vec::with_capacity(4096)).await;
                    read?;
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

        assert_eq!(got, RESPONSE);
        server.join().expect("server thread");
    });
}

#[test]
fn cancelled_connect_reclaims_tag_for_reuse() {
    let response: &'static [u8] = b"PONG-after-cancel";
    let (addr, server) = spawn_reply_server(response, 2);

    connector_exec(MAX_CONN).enter(|mut sess| {
        let (storage, mut driver) = sess.storage_and_driver();
        let connector: Conn<'_, '_> = storage.connector(&mut driver).expect("connector");
        let app = pin!(BrandCell::new(App { connector }));
        let conn = sess.storage().handle();

        drive(
            &mut sess,
            app.as_ref(),
            dope_gen::fiber!('_ => async move {
                PollOnce::new(conn.connect(addr, Default::default())).await;
            }),
        );
        let io = drive(
            &mut sess,
            app.as_ref(),
            dope_gen::fiber!('_ => async move { conn.connect(addr, Default::default()).await }),
        )
        .expect("re-connect after cancel");
        let got = ping_roundtrip(&mut sess, app.as_ref(), io);
        assert_eq!(got, response, "post-cancel connection must round-trip");
        server.join().expect("server join");
    });
}
