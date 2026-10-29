use std::{
    convert::Infallible,
    io::{self, Read as _, Write as _},
    iter,
    net::{Ipv6Addr, SocketAddr, TcpListener, TcpStream, UdpSocket},
    os::fd::AsFd as _,
    time::Duration,
};

use dope::core::{
    Errno,
    dope_sys::io::socket::record::{Fault, Record},
    driver::life::recovery::{Policy, Recovery},
    platform::{
        channel::{self, Channel},
        poll, thread,
    },
};
use dope::runtime::executor::session::Completion;
use dope_dns::config;
use dope_test::harness::scenario::run::Task as _;
use o3::alloc;

use super::{ADDRESSES, LANES, OUTBOUND, Run, compose, try_configure};
use crate::common::{cookies::Client, fixture};

#[derive(Debug)]
pub enum Failure {
    Io(io::Error),
    Kind(Option<u16>),
    Generation,
    Create(channel::Create),
    Channel(Fault),
    Record(Record),
    Allocation(alloc::Error),
    Poll(Errno),
    Source(usize),
    Peer(SocketAddr),
    Alias,
    Cookie,
}

pub const READY: u32 = 0;
pub const FOUR: u32 = 1;
pub const SIX: u32 = 2;
pub const PAUSE: u32 = 3;
pub const RESUME: u32 = 4;
pub const STOP: u32 = 5;

const COOKIE_REPLY: [u8; 15] = [0, 0, 41, 4, 208, 0, 0, 0, 0, 0, 20, 0, 10, 0, 16];
const COOKIE: &[u8; 8] = b"lsd-dns!";
const RENEWED_COOKIE: &[u8; 8] = b"new-dns!";

enum Cookie {
    Present,
    Missing,
    Renew(u8),
}

#[derive(Clone, Copy)]
struct Question {
    end: usize,
    kind: u16,
}

pub struct Stub {
    truncate: bool,
    rotate: bool,
    cookie: Cookie,
    drop: u8,
    ttls: Vec<u32>,
    alias: Option<Vec<u32>>,
    seen: u8,
    generation: u8,
}

impl Stub {
    pub fn new(
        truncate: bool,
        rotate: bool,
        drop: u8,
        ttls: &[u32],
        alias: Option<&[u32]>,
    ) -> Self {
        Self {
            truncate,
            rotate,
            cookie: Cookie::Present,
            drop,
            ttls: ttls.to_vec(),
            alias: alias.map(<[u32]>::to_vec),
            seen: 0,
            generation: 0,
        }
    }

    pub fn missing_cookie(mut self) -> Self {
        self.cookie = Cookie::Missing;
        self
    }

    pub fn renew_cookie(mut self) -> Self {
        self.cookie = Cookie::Renew(0);
        self
    }

    pub fn run(
        self,
        max: u64,
        stale: u64,
        stop_at: usize,
        delay: Option<Duration>,
    ) -> Result<Run, Failure> {
        let allow_failures = self.drop != 0;
        let (server, signal) = Server::new(self)?;
        let servers = config::Servers::try_new([server.addr()]).expect("refresh servers");
        let config = try_configure(servers, max, stale, delay).expect("refresh timeout");
        let run = Run::new(allow_failures, stop_at);
        let mut received = Vec::new();
        let prepared = fixture::try_prepare::<OUTBOUND, LANES, ADDRESSES>(config, &mut received)
            .expect("refresh layout");
        let mut finish = Completion::new();
        let tail = run
            .try_tail(&mut finish, &signal, delay)
            .map_err(Failure::Allocation)?;
        let (output, worker) = server
            .with(|| {
                match signal.recv() {
                    Record::Value(READY) => {}
                    record => {
                        let stop = signal.send(STOP);
                        return (Err(Failure::Record(record)), stop);
                    }
                }
                let mut stop = None;
                let mut outcome = None;
                prepared.try_scoped(
                    |scope| {
                        scope.lend(|install, stage| compose(tail, install, stage, &mut outcome));
                    },
                    Policy::new(
                        |report| fixture::reclaim(report).expect("refresh reclaim"),
                        |failure: Recovery<'_, '_>| -> Infallible {
                            panic!(
                                "refresh recovery: {:?}; stop: {:?}",
                                failure.cause(),
                                signal.send(STOP)
                            )
                        },
                    ),
                    |result| {
                        stop = Some(signal.send(STOP));
                        result
                            .as_ref()
                            .map_err(|error| error.kind())
                            .expect("refresh scope");
                    },
                );
                (
                    Ok(outcome.expect("refresh scope ran")),
                    stop.expect("refresh scope finished"),
                )
            })
            .expect("refresh worker start");
        let (output, stopped) = output;
        worker.expect("DNS worker joined")?;
        stopped.map_err(Failure::Channel)?;
        output?.expect("refresh application");
        assert!(
            received.is_empty(),
            "unexpected DNS receive failure: {received:?}"
        );
        Ok(run)
    }

    fn question(query: &[u8]) -> Option<Question> {
        if query.len() < 17 {
            return None;
        }
        let mut pos = 12;
        loop {
            let len = usize::from(*query.get(pos)?);
            pos += 1;
            if len == 0 {
                break;
            }
            pos = pos.checked_add(len)?;
            if pos > query.len() {
                return None;
            }
        }
        Some(Question {
            end: pos.checked_add(4)?,
            kind: u16::from_be_bytes([*query.get(pos)?, *query.get(pos + 1)?]),
        })
    }

    fn begin(
        query: &[u8],
        q: Question,
        flags: u16,
        count: u16,
        cap: usize,
    ) -> Option<(Vec<u8>, Option<[u8; 8]>)> {
        let cookie = Client::read(query, q.end).map(Client::into_bytes);
        let mut out = Vec::with_capacity(cap);
        out.extend_from_slice(query.get(..2)?);
        for value in [flags, 1, count, 0, u16::from(cookie.is_some())] {
            out.extend_from_slice(&value.to_be_bytes());
        }
        out.extend_from_slice(query.get(12..q.end)?);
        Some((out, cookie))
    }

    fn finish(mut out: Vec<u8>, cookie: Option<[u8; 8]>) -> Vec<u8> {
        if let Some(cookie) = cookie {
            out.extend_from_slice(&COOKIE_REPLY);
            out.extend_from_slice(&cookie);
            out.extend_from_slice(COOKIE);
        }
        out
    }

    pub(crate) fn truncated(query: &[u8]) -> Option<Vec<u8>> {
        let q = Self::question(query)?;
        let (out, cookie) = Self::begin(query, q, 0x8380, 0, q.end)?;
        Some(Self::finish(out, cookie))
    }

    pub(crate) fn answer(query: &[u8], epoch: u8, ttls: &[u32], rotate: bool) -> Option<Vec<u8>> {
        let q = Self::question(query)?;
        let v6 = Ipv6Addr::LOCALHOST.octets();
        match (q.kind, epoch, rotate) {
            (1, 0, _) => Self::values(query, q, &[&[10, 1, 105, 164]], ttls),
            (1, _, true) => Self::values(query, q, &[&[10, 1, 84, 243], &[10, 1, 108, 45]], ttls),
            (1, _, false) => Self::values(query, q, &[&[10, 1, 84, 243]], ttls),
            (28, _, true) => Self::values(query, q, &[], ttls),
            (28, _, false) => Self::values(query, q, &[&v6], ttls),
            _ => None,
        }
    }

    fn values(query: &[u8], q: Question, values: &[&[u8]], ttls: &[u32]) -> Option<Vec<u8>> {
        let cap = values.iter().try_fold(q.end, |cap, value| {
            cap.checked_add(12usize.checked_add(value.len())?.checked_mul(ttls.len())?)
        })?;
        let count = u16::try_from(values.len().checked_mul(ttls.len())?).ok()?;
        let (mut out, cookie) = Self::begin(query, q, 0x8180, count, cap)?;
        for value in values {
            for ttl in ttls {
                for word in [0xc00c, q.kind, 1] {
                    out.extend_from_slice(&word.to_be_bytes());
                }
                out.extend_from_slice(&ttl.to_be_bytes());
                out.extend_from_slice(&u16::try_from(value.len()).ok()?.to_be_bytes());
                out.extend_from_slice(value);
            }
        }
        Some(Self::finish(out, cookie))
    }

    fn reply(&mut self, query: &[u8], signal: &Channel) -> Result<Option<Vec<u8>>, Failure> {
        let question = Self::question(query);
        let kind = question.map(|q| q.kind);
        let challenge = if self.generation == 0 || matches!(self.cookie, Cookie::Present) {
            false
        } else {
            let q = question.ok_or(Failure::Cookie)?;
            let tail = query.get(q.end..).ok_or(Failure::Cookie)?;
            if tail.len() != COOKIE_REPLY.len() + 8 + COOKIE.len()
                || !tail.starts_with(&COOKIE_REPLY)
            {
                return Err(Failure::Cookie);
            }
            match &mut self.cookie {
                Cookie::Missing if tail.ends_with(COOKIE) => false,
                Cookie::Renew(seen) if self.generation == 1 && tail.ends_with(COOKIE) => {
                    let family = match kind {
                        Some(1) => 1,
                        Some(28) => 2,
                        _ => return Err(Failure::Kind(kind)),
                    };
                    if *seen & family != 0 {
                        return Err(Failure::Cookie);
                    }
                    *seen |= family;
                    true
                }
                Cookie::Renew(seen) if *seen != 0 && tail.ends_with(RENEWED_COOKIE) => false,
                _ => return Err(Failure::Cookie),
            }
        };
        if let (Some(1), Some(ttls)) = (kind, &self.alias) {
            if query[12..].starts_with(b"\x04edge") {
                signal.send(PAUSE).map_err(Failure::Channel)?;
                match signal.recv() {
                    Record::Value(RESUME) => {}
                    record => return Err(Failure::Record(record)),
                }
                self.alias = None;
            } else {
                return Ok((|| {
                    let q = Self::question(query)?;
                    let count = u16::try_from(ttls.len()).ok()?;
                    let (mut out, cookie) = Self::begin(query, q, 0x8180, count, q.end)?;
                    for ttl in ttls {
                        out.extend_from_slice(&[0xc0, 12, 0, 5, 0, 1]);
                        out.extend_from_slice(&ttl.to_be_bytes());
                        out.extend_from_slice(b"\0\x0e\x04edge\x07example\0");
                    }
                    Some(Self::finish(out, cookie))
                })());
            }
        }
        let mut reply = if challenge {
            let q = question.ok_or(Failure::Cookie)?;
            let (out, cookie) = Self::begin(query, q, 0x8187, 0, q.end).ok_or(Failure::Cookie)?;
            let mut reply = Self::finish(out, cookie);
            reply[q.end + 5] = 1;
            Some(reply)
        } else if self.generation != 0 && self.drop != 0 {
            self.drop -= 1;
            let family = match kind {
                Some(1) => FOUR,
                Some(28) => SIX,
                _ => return Err(Failure::Kind(kind)),
            };
            signal.send(family).map_err(Failure::Channel)?;
            None
        } else if matches!(self.cookie, Cookie::Missing) && self.generation == 1 {
            let q = question.ok_or(Failure::Cookie)?;
            let (mut reply, _) = Self::begin(query, q, 0x8180, 0, q.end).ok_or(Failure::Cookie)?;
            reply[11] = 0;
            Some(reply)
        } else if self.truncate {
            Self::truncated(query)
        } else {
            let ttls = if self.generation == 0 {
                &self.ttls[..]
            } else {
                &[30]
            };
            Self::answer(query, self.generation, ttls, self.rotate)
        };
        if self.generation != 0
            && matches!(self.cookie, Cookie::Renew(_))
            && let Some(reply) = &mut reply
        {
            *reply.last_chunk_mut::<8>().ok_or(Failure::Cookie)? = *RENEWED_COOKIE;
        }
        if reply.is_some() && self.rotate && !challenge {
            self.seen |= match kind {
                Some(1) => 1,
                Some(28) => 2,
                _ => 0,
            };
            if self.seen == 3 {
                self.seen = 0;
                self.generation = self.generation.checked_add(1).ok_or(Failure::Generation)?;
            }
        }
        Ok(reply)
    }
}

pub struct Server {
    addr: SocketAddr,
    tcp: TcpListener,
    udp: UdpSocket,
    signal: Channel,
    stub: Stub,
}

impl Server {
    fn new(stub: Stub) -> Result<(Self, Channel), Failure> {
        let tcp = TcpListener::bind("127.0.0.1:0").map_err(Failure::Io)?;
        let addr = tcp.local_addr().map_err(Failure::Io)?;
        let udp = UdpSocket::bind(addr).map_err(Failure::Io)?;
        tcp.set_nonblocking(true).map_err(Failure::Io)?;
        udp.set_nonblocking(true).map_err(Failure::Io)?;
        let (signal, parent) = Channel::pair().map_err(Failure::Create)?;
        Ok((
            Self {
                addr,
                tcp,
                udp,
                signal,
                stub,
            },
            parent,
        ))
    }

    pub const fn addr(&self) -> SocketAddr {
        self.addr
    }

    fn stream(mut stream: TcpStream) -> io::Result<()> {
        stream.set_read_timeout(Some(Duration::from_millis(100)))?;
        loop {
            let mut prefix = [0; 2];
            if let Err(error) = stream.read_exact(&mut prefix) {
                return match error.kind() {
                    io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::WouldBlock
                    | io::ErrorKind::TimedOut => Ok(()),
                    _ => Err(error),
                };
            }
            let mut query = vec![0; usize::from(u16::from_be_bytes(prefix))];
            stream.read_exact(&mut query)?;
            let Some(reply) = Stub::answer(&query, 0, &[30], false) else {
                return Err(io::ErrorKind::InvalidData.into());
            };
            let Ok(len) = u16::try_from(reply.len()) else {
                return Err(io::ErrorKind::InvalidData.into());
            };
            let write = stream
                .write_all(&len.to_be_bytes())
                .and_then(|()| stream.write_all(&reply));
            if let Err(error) = write {
                return match error.kind() {
                    io::ErrorKind::BrokenPipe
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::NotConnected => Ok(()),
                    _ => Err(error),
                };
            }
        }
    }
}

impl thread::Task for Server {
    type Output = Result<(), Failure>;

    fn run(self) -> Self::Output {
        let Self {
            tcp,
            udp,
            signal,
            mut stub,
            ..
        } = self;
        let mut events = poll::Set::try_new(
            [signal.as_fd(), udp.as_fd(), tcp.as_fd()].into_iter(),
            iter::empty(),
        )
        .map_err(Failure::Allocation)?;
        signal.send(READY).map_err(Failure::Channel)?;
        let mut packet = [0; 2048];
        loop {
            let index = match events.poll(-1) {
                Ok(Some(ready)) => ready.index(),
                Ok(None) => continue,
                Err(error) if error.is_intr() => continue,
                Err(error) => return Err(Failure::Poll(error)),
            };
            match index {
                0 => {
                    return match signal.recv() {
                        Record::Value(STOP) | Record::Closed => Ok(()),
                        record => Err(Failure::Record(record)),
                    };
                }
                1 => match udp.recv_from(&mut packet) {
                    Ok((len, peer)) => {
                        if let Some(reply) = stub.reply(&packet[..len], &signal)? {
                            udp.send_to(&reply, peer).map_err(Failure::Io)?;
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                    Err(error) => return Err(Failure::Io(error)),
                },
                2 => match tcp.accept() {
                    Ok((stream, peer)) => {
                        if !peer.ip().is_loopback() {
                            return Err(Failure::Peer(peer));
                        }
                        if matches!(stub.cookie, Cookie::Renew(_)) {
                            return Err(Failure::Cookie);
                        }
                        Self::stream(stream).map_err(Failure::Io)?;
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                    Err(error) => return Err(Failure::Io(error)),
                },
                _ => return Err(Failure::Source(index)),
            }
        }
    }
}
