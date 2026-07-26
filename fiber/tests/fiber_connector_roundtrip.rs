use dope_test as common;

use std::cell::Cell;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::thread::JoinHandle;
use std::time::Duration;

extern crate dope;
use dope::runtime::dispatcher::Dispatcher;
use dope::runtime::executor::{Executor, Session};
use dope::runtime::profile::Balanced;
use dope_fiber::abi::Fiber;
use dope_fiber::io::Io;
use dope_fiber::net::connector::{Connector, ConnectorHandle, ConnectorPort, ConnectorPortFactory};
use dope_net::tcp::Tcp;
use dope_net::wire::identity::Identity;
use dope_net::wire::send::{Plain, Prepared, SendBuf, Storage, Vectored};
use dope_net::wire::{ReadyOpen, Reclaim, RuntimeLimits, Wire};
use o3::buffer::{Borrowed, Bytes};
use o3::cell::BrandCell;

use common::drive;

const ID: u8 = 0;
const MAX_CONN: usize = 8;

type Conn<'scope, 'd> = Connector<'scope, 'd, ID, Tcp, Identity>;
type ConnIo<'scope, 'd> = Io<'d, ConnectorHandle<'scope, 'd, Tcp>>;

struct RecvGatedWire {
    established: bool,
    attempts: Rc<Cell<usize>>,
}

impl Wire for RecvGatedWire {
    type InitConfig = Rc<Cell<usize>>;
    type RuntimeContext = Rc<Cell<usize>>;
    type Open<'a> = ReadyOpen<Self>;
    type Recv<'a> = Bytes<Borrowed<'a>>;
    type SendStorage = SendBuf<1024>;

    const RECLAIM: Reclaim = Reclaim::OnSubmit;

    fn runtime_context(
        _: RuntimeLimits,
        config: Self::InitConfig,
    ) -> std::io::Result<Self::RuntimeContext> {
        Ok(config)
    }

    fn prepare_open(runtime: &mut Self::RuntimeContext) -> Option<Self::Open<'_>> {
        Some(ReadyOpen::new(
            Self {
                established: false,
                attempts: runtime.clone(),
            },
            SendBuf::new(),
        ))
    }

    fn holds_plain(&self, send: &Self::SendStorage) -> bool {
        !send.is_empty()
    }

    fn process_recv<'a>(
        &mut self,
        _: &mut Self::RuntimeContext,
        bytes: &'a [u8],
    ) -> Option<Self::Recv<'a>> {
        if !self.established {
            self.established = true;
            None
        } else {
            Some(Bytes::<Borrowed<'a>>::from(bytes))
        }
    }

    fn prepare_send<'a>(
        &'a mut self,
        mut send: Storage<'a, Self::SendStorage>,
        plain: Plain<'a>,
    ) -> Prepared<'a> {
        self.attempts.set(self.attempts.get() + 1);
        if !self.established {
            return send.empty(0);
        }
        let n = send.spare_capacity().min(plain.len());
        send.extend_from_slice(&plain.as_slice()[..n]);
        send.buffered(n)
    }

    fn prepare_send_vectored<'a>(
        &'a mut self,
        mut send: Storage<'a, Self::SendStorage>,
        plain: Vectored<'a>,
    ) -> Prepared<'a> {
        self.attempts.set(self.attempts.get() + 1);
        if !self.established {
            return send.empty(0);
        }
        let mut consumed = 0;
        for bytes in plain.iter() {
            let n = send.spare_capacity().min(bytes.len());
            send.extend_from_slice(&bytes[..n]);
            consumed += n;
            if n != bytes.len() {
                break;
            }
        }
        send.buffered(consumed)
    }

    fn after_send<'a>(
        &'a mut self,
        mut send: Storage<'a, Self::SendStorage>,
        n: usize,
    ) -> Prepared<'a> {
        send.consume(n);
        send.buffered(0)
    }

    fn flush_pending<'a>(&'a mut self, send: Storage<'a, Self::SendStorage>) -> Prepared<'a> {
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

fn connector_exec(max_connections: usize) -> Executor<ConnectorPortFactory<Tcp>> {
    let cfg = dope::driver::Config::for_tcp_profile::<Balanced>(max_connections);
    Executor::new(cfg).expect("executor").with_storage_factory(
        ConnectorPort::<Tcp>::factory(max_connections).expect("connector capacity"),
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

fn ping_roundtrip<'scope, 'd, D: Dispatcher<'d>>(
    sess: &mut Session<'scope, 'd, ConnectorPort<'d, Tcp>>,
    app: Pin<&BrandCell<'d, D>>,
    io: ConnIo<'scope, 'd>,
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
        let (next, buf, n) = drive(
            sess,
            app,
            dope_gen::fiber!('_ => async move {
                let mut io = owner.take().expect("io owner");
                let (result, buf) = io.read(vec![0; 16]).await;
                let n = result?;
                Ok::<_, std::io::Error>((io, buf, n))
            }),
        )
        .expect("read response");
        io = next;
        if n == 0 {
            break;
        }
        got.extend_from_slice(&buf[..n]);
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
