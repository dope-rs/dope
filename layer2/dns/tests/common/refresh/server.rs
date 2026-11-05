use std::{
    convert::Infallible,
    net::{Ipv6Addr, SocketAddr},
    num::TryFromIntError,
    time::Duration,
};

use dope::core::driver::life::recovery::{Policy, Recovery};
use dope::runtime::executor::session::Completion;
use dope_dns::config;
use dope_test::harness::peer::{self, TcpAcceptor, udp};

use super::{ADDRESSES, LANES, OUTBOUND, Run, Tail, compose, try_configure};
use crate::common::{
    cookies::Client,
    fixture,
    stream::{Connection, Event},
};

#[derive(Debug)]
pub enum Failure {
    Io(peer::Error),
    Packet,
    Length(TryFromIntError),
    Kind(Option<u16>),
    Generation,
    Alias,
    Cookie,
    Peer,
    Phase,
}

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

enum Reply {
    Send(Vec<u8>),
    Pause(Vec<u8>),
    Drop(u8),
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
        let server = Server::new(self)?;
        let servers = config::Servers::try_new([server.addr()]).expect("refresh servers");
        let config = try_configure(servers, max, stale, delay).expect("refresh timeout");
        let run = Run::new(allow_failures, stop_at);
        let mut received = Vec::new();
        let prepared = fixture::try_prepare::<OUTBOUND, LANES, ADDRESSES>(config, &mut received)
            .expect("refresh layout");
        let mut finish = Completion::new();
        let tail = Tail {
            finish: &mut finish,
            run: run.clone(),
            server,
            delay,
        };
        let mut outcome = None;
        prepared.try_scoped(
            |scope| {
                scope.lend(|install, stage| compose(tail, install, stage, &mut outcome));
            },
            Policy::new(
                |report| fixture::reclaim(report).expect("refresh reclaim"),
                |failure: Recovery<'_, '_>| -> Infallible {
                    panic!("refresh recovery: {:?}", failure.cause())
                },
            ),
            |result| {
                result
                    .as_ref()
                    .map_err(|error| error.kind())
                    .expect("refresh scope");
            },
        );
        outcome
            .expect("refresh scope ran")
            .expect("refresh application");
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

    fn reply(&mut self, query: &[u8]) -> Result<Option<Reply>, Failure> {
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
        let mut pause = false;
        if let (Some(1), Some(ttls)) = (kind, &self.alias) {
            if query[12..].starts_with(b"\x04edge") {
                pause = true;
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
                })()
                .map(Reply::Send));
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
                Some(1) => 1,
                Some(28) => 2,
                _ => return Err(Failure::Kind(kind)),
            };
            return Ok(Some(Reply::Drop(family)));
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
        Ok(reply.map(|bytes| {
            if pause {
                Reply::Pause(bytes)
            } else {
                Reply::Send(bytes)
            }
        }))
    }
}

pub struct Server {
    addr: SocketAddr,
    tcp: TcpAcceptor<'static>,
    udp: udp::Socket<'static>,
    stream: Option<Connection>,
    stub: Stub,
}

impl Server {
    fn new(stub: Stub) -> Result<Self, Failure> {
        let (addr, tcp, udp) = crate::common::socket::bind().map_err(Failure::Io)?;
        let tcp = TcpAcceptor::try_adopt(tcp).map_err(Failure::Io)?;
        let udp = udp::Socket::try_adopt(udp).map_err(Failure::Io)?;
        udp.nonblock(true);
        Ok(Self {
            addr,
            tcp,
            udp,
            stream: None,
            stub,
        })
    }

    pub const fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub(super) fn send(&self, reply: &[u8], peer: SocketAddr) -> Result<(), Failure> {
        self.udp
            .send_to(reply, peer)
            .map(|_| ())
            .map_err(Failure::Io)
    }

    pub(super) fn poll(&mut self, lost: &mut u8) -> Result<Option<(Vec<u8>, SocketAddr)>, Failure> {
        let mut packet = [0; 2048];
        match self.udp.recv_from(&mut packet) {
            Ok((len, peer)) => match self.stub.reply(&packet[..len])? {
                Some(Reply::Send(reply)) => self.send(&reply, peer)?,
                Some(Reply::Pause(reply)) => return Ok(Some((reply, peer))),
                Some(Reply::Drop(family)) => *lost |= family,
                None => {}
            },
            Err(peer::Error::Syscall(error)) if error.is_block() || error.is_intr() => {}
            Err(error) => return Err(Failure::Io(error)),
        }
        if self.stream.is_none()
            && let Some(stream) = self.tcp.try_accept().map_err(Failure::Io)?
        {
            if matches!(self.stub.cookie, Cookie::Renew(_)) {
                return Err(Failure::Cookie);
            }
            self.stream = Some(Connection::new(stream));
        }
        if let Some(stream) = &mut self.stream {
            match stream.poll()? {
                std::task::Poll::Ready(Event::Query(query)) => stream.reply(query)?,
                std::task::Poll::Ready(Event::Closed(_)) => self.stream = None,
                std::task::Poll::Ready(Event::Written) | std::task::Poll::Pending => {}
            }
        }
        Ok(None)
    }
}
