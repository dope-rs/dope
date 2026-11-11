use crate::common::runtime;
use dope_quic::conn::id;
use dope_quic::conn::life::accept;
use dope_quic::conn::life::options::Options;
use dope_quic::proto::epoch;
use dope_quic::runtime::mux::builder::Builder;
use dope_quic::runtime::mux::handler;
use dope_quic::runtime::mux::handler::hook::life::{create::Unit, ready};
use dope_quic::runtime::mux::handler::hook::set::{On, Set};
use dope_quic::runtime::mux::handler::hook::wire::cert;
use dope_quic::runtime::mux::router::Router;
use dope_test::harness::peer::{self as native, udp::Socket};
use dope_tls::config::Ext as _;
use o3::buffer::storage;
use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::{net, pin, time};

#[derive(Default)]
pub struct Events {
    pub established: Vec<id::Handle>,
    pub certificates: usize,
}

impl handler::Handler<0, Vec<u8>, storage::Shared> for Events {
    type Connection = ();
    type Create = Unit;
    type Events = Set<On>;
}

impl ready::Sink<0, Vec<u8>, storage::Shared, id::Static> for Events {
    fn established(event: ready::Event<'_, Self, 0, Vec<u8>, storage::Shared, id::Static>) {
        let (mut events, _, handle, _) = event.into();
        events.established.push(handle);
    }
}

impl cert::Sink<0, id::Static> for Events {
    fn certificate(event: cert::Event<'_, '_, Self, 0, id::Static>) {
        let (mut events, _, _, _) = event.into_parts();
        events.certificates += 1;
    }
}

pub struct Identity {
    pub key: Vec<u8>,
    pub chain: [Vec<u8>; 1],
}

#[derive(Debug)]
pub enum Error {
    Socket(native::Error),
    Runtime(runtime::Error),
    Sent { expected: usize, actual: usize },
}

#[derive(Debug)]
pub enum Setup {
    Tls(dope_tls::Error),
    Runtime(runtime::Error),
    Socket(native::Error),
}

pub const COMMANDS: [&[&str]; 4] = [
    &[
        "genpkey",
        "-algorithm",
        "EC",
        "-pkeyopt",
        "ec_paramgen_curve:P-256",
        "-out",
        "key.pem",
    ],
    &[
        "pkcs8", "-topk8", "-nocrypt", "-in", "key.pem", "-outform", "DER", "-out", "key.der",
    ],
    &[
        "req",
        "-new",
        "-x509",
        "-key",
        "key.pem",
        "-sha256",
        "-days",
        "1",
        "-subj",
        "/CN=127.0.0.1",
        "-addext",
        "subjectAltName=IP:127.0.0.1",
        "-addext",
        "basicConstraints=critical,CA:FALSE",
        "-addext",
        "keyUsage=critical,digitalSignature",
        "-addext",
        "extendedKeyUsage=serverAuth,clientAuth",
        "-out",
        "cert.pem",
    ],
    &[
        "x509", "-in", "cert.pem", "-outform", "DER", "-out", "cert.der",
    ],
];

pub struct Server {
    pub router: Router<Events>,
    socket: Socket<'static>,
    packet: Vec<u8>,
    pub received: usize,
    pub sent: usize,
}

impl Server {
    pub fn new(input: &Identity) -> Result<Self, Setup> {
        let mut tls = ServerConfig::try_tls13()
            .map_err(Setup::Tls)?
            .with_no_client_auth()
            .with_single_cert(
                input
                    .chain
                    .iter()
                    .cloned()
                    .map(CertificateDer::from)
                    .collect(),
                PrivatePkcs8KeyDer::from(input.key.clone()).into(),
            )
            .map_err(dope_tls::Error::Tls)
            .map_err(Setup::Tls)?;
        tls.alpn_protocols = vec![b"h3".to_vec()];
        let options = Options::builder()
            .try_build()
            .map_err(runtime::Error::Options)
            .map_err(Setup::Runtime)?;
        let auth = accept::Authority::new(tls, options)
            .map_err(runtime::Error::Connect)
            .map_err(Setup::Runtime)?;
        let router = Builder::server(Events::default(), auth)
            .build()
            .map_err(runtime::Error::Storage)
            .map_err(Setup::Runtime)?;
        let socket = Socket::bind((net::Ipv4Addr::LOCALHOST, 0).into()).map_err(Setup::Socket)?;
        socket
            .read_wait(Some(time::Duration::from_millis(2)))
            .map_err(Setup::Socket)?;
        Ok(Self {
            router,
            socket,
            packet: vec![0; u16::MAX as usize],
            received: 0,
            sent: 0,
        })
    }

    pub fn addr(&self) -> Result<net::SocketAddr, native::Error> {
        self.socket.local_addr()
    }

    pub fn poll(&mut self) -> Result<(), Error> {
        let now = time::Instant::now();
        match self.socket.recv_from(&mut self.packet) {
            Ok((length, from)) => {
                self.received += 1;
                pin::Pin::new(&mut self.router)
                    .observe(now, |mut turn| {
                        turn.protocol()
                            .recv(from, &mut self.packet[..length], epoch::WALL_CLOCK)
                    })
                    .map_err(runtime::Error::Receive)
                    .map_err(Error::Runtime)?;
            }
            Err(native::Error::TimedOut) => {}
            Err(native::Error::Syscall(cause)) if cause.is_block() => {}
            Err(cause) => return Err(Error::Socket(cause)),
        }
        pin::Pin::new(&mut self.router)
            .observe(now, |mut turn| turn.output().drive())
            .map_err(runtime::Error::Output)
            .map_err(Error::Runtime)?;
        let outgoing: Vec<_> = pin::Pin::new(&mut self.router)
            .observe(now, |mut turn| turn.output().drain().collect());
        for mut outgoing in outgoing {
            let address = outgoing.addr();
            for segment in outgoing.segments_mut() {
                self.sent += 1;
                let actual = self
                    .socket
                    .send_to(segment, address)
                    .map_err(Error::Socket)?;
                if actual != segment.len() {
                    return Err(Error::Sent {
                        expected: segment.len(),
                        actual,
                    });
                }
            }
            pin::Pin::new(&mut self.router).output().recycle(outgoing);
        }
        Ok(())
    }
}
