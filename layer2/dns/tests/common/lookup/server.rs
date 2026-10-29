use std::{
    io::{self, Read, Write},
    iter, mem,
    net::{SocketAddr, TcpListener, TcpStream, UdpSocket},
    thread::sleep,
    time::Duration,
};

use dope::core::platform::thread;

use crate::common::{cookies::Client, lookup::failure::Server};

const A: u16 = 1;
const CNAME: u16 = 5;
const COOKIE: u16 = 10;
const QOFF: usize = 12;
const LIMIT: Duration = Duration::from_secs(3);
const COOKIE_BYTES: [u8; 32] = [b's'; 32];

#[derive(Clone, Copy)]
struct Query<B> {
    buf: B,
    end: usize,
    kind: u16,
}

#[derive(Clone, Copy)]
pub enum Reply<'a> {
    Addrs(&'a [[u8; 4]], bool, bool),
    Alias(Option<[u8; 4]>, &'a [u8]),
    Aliases([&'a [u8]; 2]),
    Empty,
    Cut,
    Refused,
    Name,
}

impl<B: AsRef<[u8]>> Query<B> {
    fn parse(buf: B) -> Result<Self, Server> {
        let bytes = buf.as_ref();
        if bytes.get(2..QOFF) != Some(&[1, 0, 0, 1, 0, 0, 0, 0, 0, 1]) {
            return Err(Server::Header);
        }
        let mut end = QOFF;
        loop {
            let value = *bytes.get(end).ok_or(Server::Offset(end))?;
            if value == 0 {
                break;
            }
            if value > 63 {
                return Err(Server::Label { offset: end, value });
            }
            end += usize::from(value) + 1;
        }
        let tail = bytes.get(end + 1..end + 5).ok_or(Server::Offset(end))?;
        let kind = u16::from_be_bytes([tail[0], tail[1]]);
        end += 5;
        if end == QOFF + 255 + 4 {
            let mut expected = Vec::new();
            for (byte, len) in [(b'a', 63), (b'b', 63), (b'c', 63), (b'd', 61)] {
                expected.push(len);
                expected.extend(iter::repeat_n(byte, usize::from(len)));
            }
            expected.push(0);
            if bytes[QOFF..end - 4] != expected {
                return Err(Server::Name);
            }
        }
        Ok(Self { buf, end, kind })
    }

    fn client(&self) -> Result<[u8; 8], Server> {
        let client = Client::read(self.buf.as_ref(), self.end).ok_or(Server::Cookie)?;
        Ok(client.into_bytes())
    }

    fn require_kind(&self, expected: u16) -> Result<(), Server> {
        if self.kind == expected {
            Ok(())
        } else {
            Err(Server::Kind {
                actual: self.kind,
                expected,
            })
        }
    }

    fn reply(&self, reply: Reply<'_>, auth: bool) -> Result<Vec<u8>, Server> {
        let buf = self.buf.as_ref();
        let client = auth.then(|| self.client()).transpose()?;
        let edns = matches!(reply, Reply::Addrs(_, _, true));
        let count = match reply {
            Reply::Addrs(vals, root, _) => vals.len() + usize::from(root),
            Reply::Alias(addr, _) => 1 + usize::from(addr.is_some()),
            Reply::Aliases(_) => 3,
            Reply::Empty | Reply::Cut | Reply::Refused | Reply::Name => 0,
        };
        let count = u8::try_from(count).map_err(Server::Length)?;
        let code = match reply {
            Reply::Refused => 5,
            Reply::Name => 3,
            _ => 0,
        };
        let cut = u16::from(matches!(reply, Reply::Cut)) << 9;
        let flags: u16 = 0x8180 | cut | code;
        let mut out = Vec::from([buf[0], buf[1]]);
        out.extend_from_slice(&flags.to_be_bytes());
        out.extend_from_slice(&[0, 1, 0, count, 0, 0, 0, u8::from(client.is_some() || edns)]);
        out.extend_from_slice(&buf[QOFF..self.end]);
        let owner = [0xc0, QOFF as u8];
        match reply {
            Reply::Addrs(vals, root, _) => {
                if root {
                    record(&mut out, &[0], A, &[198, 51, 100, 9])?;
                }
                for value in vals {
                    record(&mut out, &owner, A, value)?;
                }
            }
            Reply::Alias(addr, alias) => {
                let pos = out.len() + 12;
                record(&mut out, &owner, CNAME, alias)?;
                if let Some(addr) = addr {
                    record(&mut out, &ptr(pos)?, A, &addr)?;
                }
            }
            Reply::Aliases(aliases) => {
                let pos = out.len() + 12;
                for alias in aliases {
                    record(&mut out, &owner, CNAME, alias)?;
                }
                record(&mut out, &ptr(pos)?, A, &[192, 0, 2, 2])?;
            }
            Reply::Empty | Reply::Cut | Reply::Refused | Reply::Name => {}
        }
        if client.is_some() || edns {
            out.extend_from_slice(&[0, 0, 41, 4, 208, 0, 0, 0, 0, 0]);
            let auth = u8::from(client.is_some());
            out.push(44 * auth + 4 * u8::from(edns));
            if let Some(key) = client {
                out.extend_from_slice(&[0, COOKIE as u8, 0, 40]);
                out.extend_from_slice(&key);
                out.extend_from_slice(&COOKIE_BYTES);
            }
            if edns {
                out.extend_from_slice(&[0xfd, 0xe9, 0, 0]);
            }
        }
        Ok(out)
    }
}

fn record(out: &mut Vec<u8>, owner: &[u8], kind: u16, data: &[u8]) -> Result<(), Server> {
    out.extend_from_slice(owner);
    out.extend_from_slice(&kind.to_be_bytes());
    out.extend_from_slice(&[0, 1, 0, 0, 0, 30]);
    let len = u16::try_from(data.len()).map_err(Server::Length)?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(data);
    Ok(())
}

fn ptr(pos: usize) -> Result<[u8; 2], Server> {
    if pos >= 1 << 14 {
        return Err(Server::Offset(pos));
    }
    let pos = u16::try_from(pos).map_err(Server::Length)?;
    Ok((0xc000 | pos).to_be_bytes())
}

fn cookie<'a>(buf: &'a mut [u8], key: &[u8; 8]) -> Result<&'a mut [u8; 50], Server> {
    let pos = buf
        .windows(12)
        .rposition(|part| part.starts_with(&[0, COOKIE as u8, 0, 40]) && part.ends_with(key))
        .ok_or(Server::Cookie)?;
    buf.get_mut(pos.checked_sub(6).ok_or(Server::Cookie)?..)
        .and_then(<[u8]>::first_chunk_mut::<50>)
        .ok_or(Server::Cookie)
}

#[derive(Clone, Copy)]
pub enum Reject {
    Wrong,
    Duplicate,
    Trailing,
    Forward,
    AliasConflict,
}

impl Reject {
    fn apply(self, buf: &mut Vec<u8>, query: Query<&[u8]>) -> Result<(), Server> {
        match self {
            Self::Wrong => {
                cookie(buf, &query.client()?)?[10] ^= 1;
            }
            Self::Duplicate => {
                let option = cookie(buf, &query.client()?)?;
                let copy = option[6..].to_vec();
                option[4..6].copy_from_slice(&88u16.to_be_bytes());
                buf.extend_from_slice(&copy);
            }
            Self::Trailing => buf.push(0),
            Self::Forward => {
                let pos = buf
                    .windows(2)
                    .position(|part| part == [0xc0, QOFF as u8])
                    .ok_or(Server::Pointer)?;
                *buf.get_mut(pos..)
                    .and_then(<[u8]>::first_chunk_mut::<2>)
                    .ok_or(Server::Pointer)? = ptr(pos)?;
            }
            Self::AliasConflict => {
                *buf = query.reply(
                    Reply::Aliases([b"\x04edge\x07example\0", b"\x04next\x07example\0"]),
                    true,
                )?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub enum Cookie {
    Missing,
    Bad,
    Renew,
}

#[derive(Clone, Copy)]
pub enum Mode<'a> {
    Reply(Reply<'a>),
    Stream(Reply<'a>),
    Follow(&'a [u8]),
    Oversized,
    Refused,
    Reject(Reject),
    Delay(Duration),
    Malformed(bool),
    Close(bool),
    Partial,
    Cookie(Cookie),
    ExtendedCode(u8),
}

pub struct Peer<'a> {
    pub addr: SocketAddr,
    listen: TcpListener,
    udp: UdpSocket,
    mode: Mode<'a>,
    rounds: usize,
}

impl<'a> Peer<'a> {
    pub fn new(mode: Mode<'a>, rounds: usize) -> io::Result<Self> {
        let (addr, listen, udp) = bind()?;
        udp.set_read_timeout(Some(LIMIT))?;
        Ok(Self {
            addr,
            listen,
            udp,
            mode,
            rounds,
        })
    }
}

impl thread::Task for Peer<'_> {
    type Output = Result<usize, Server>;

    fn run(self) -> Self::Output {
        self.mode.run(&self.udp, &self.listen, self.rounds)
    }
}

impl Mode<'_> {
    fn run(
        mut self,
        udp: &UdpSocket,
        listen: &TcpListener,
        rounds: usize,
    ) -> Result<usize, Server> {
        let mut buf = [0; 512];
        let mut done = 0;
        let mut clients = [None; 2];
        let mut renewed = [false; 2];
        while done < rounds {
            let (len, peer) = udp.recv_from(&mut buf).map_err(Server::Io)?;
            let query = Query::parse(&buf[..len])?;
            let paired = query.buf.len() == query.end + 11 + 4 + 8 + COOKIE_BYTES.len();
            if matches!(self, Self::Cookie(_)) {
                let client = query.client()?;
                if paired {
                    if !clients.contains(&Some(client)) || !query.buf.ends_with(&COOKIE_BYTES) {
                        return Err(Server::Cookie);
                    }
                } else {
                    if clients.contains(&Some(client)) {
                        return Err(Server::Cookie);
                    }
                    if let Some(slot) = clients.iter_mut().find(|slot| slot.is_none()) {
                        *slot = Some(client);
                    }
                }
            }
            if query.end == QOFF + 255 + 4 && done == 2 {
                if len != 326 {
                    return Err(Server::Size {
                        actual: len,
                        expected: 326,
                    });
                }
                if !query.buf.ends_with(&COOKIE_BYTES) {
                    return Err(Server::Cookie);
                }
            }
            let reply = match self {
                Self::Oversized => {
                    let actual = udp.send_to(&[0; 65_507], peer).map_err(Server::Io)?;
                    if actual != 65_507 {
                        return Err(Server::Size {
                            actual,
                            expected: 65_507,
                        });
                    }
                    if query.kind == A {
                        Reply::Addrs(&[[192, 0, 2, 11]], false, false)
                    } else {
                        Reply::Empty
                    }
                }
                Self::Reply(reply) if query.kind == A || matches!(reply, Reply::Name) => reply,
                Self::Reply(_) => Reply::Empty,
                Self::Follow(alias) if query.kind == A => {
                    self = Self::Reply(Reply::Addrs(&[[192, 0, 2, 12]], false, false));
                    Reply::Alias(None, alias)
                }
                Self::Follow(_) => Reply::Empty,
                Self::Refused if query.kind == A => {
                    self = Self::Reply(Reply::Addrs(&[[192, 0, 2, 4]], false, false));
                    Reply::Refused
                }
                Self::Refused => Reply::Empty,
                Self::Reject(mode) if query.kind == A => {
                    let mut stale =
                        query.reply(Reply::Addrs(&[[192, 0, 2, 9]], false, false), true)?;
                    mode.apply(&mut stale, query)?;
                    udp.send_to(&stale, peer).map_err(Server::Io)?;
                    Reply::Addrs(&[[192, 0, 2, 10]], false, false)
                }
                Self::Reject(_) => Reply::Empty,
                Self::Delay(delay) => {
                    let edge = !query.buf[QOFF..query.end - 4]
                        .windows(4)
                        .any(|part| part == b"edge");
                    sleep(delay);
                    Reply::Alias(
                        None,
                        if edge {
                            b"\x04edge\x07example\0"
                        } else {
                            b"\x04next\x07example\0"
                        },
                    )
                }
                Self::Cookie(_) if query.kind == A => Reply::Addrs(&[[192, 0, 2, 8]], false, false),
                Self::Partial => Reply::Cut,
                Self::Stream(_) if query.kind == A => Reply::Cut,
                Self::Stream(_) => Reply::Empty,
                Self::Malformed(_) | Self::Close(_) if query.kind == A => Reply::Cut,
                Self::Malformed(_) | Self::Close(_) | Self::Cookie(_) | Self::ExtendedCode(_) => {
                    Reply::Empty
                }
            };
            let auth = !matches!(self, Self::Cookie(Cookie::Missing));
            let mut reply = query.reply(reply, auth)?;
            let renew = match self {
                Self::Cookie(Cookie::Bad) => true,
                Self::Cookie(Cookie::Renew) => {
                    !mem::replace(&mut renewed[usize::from(query.kind != A)], true)
                }
                _ => false,
            };
            if renew {
                cookie(&mut reply, &query.client()?)?[0] = 1;
                let flags = reply.get_mut(3).ok_or(Server::Header)?;
                *flags = *flags & 0xf0 | 7;
            }
            if let Self::ExtendedCode(code) = self {
                cookie(&mut reply, &query.client()?)?[0] = code;
            }
            udp.send_to(&reply, peer).map_err(Server::Io)?;
            done += 1;
        }
        match self {
            Self::Stream(reply) => {
                let mut conn = listen.accept().map_err(Server::Io)?.0;
                let query = read(&mut conn)?;
                write(&mut conn, &query.reply(reply, false)?)?;
            }
            Self::Malformed(retry) => {
                let mut conn = listen.accept().map_err(Server::Io)?.0;
                let bad = read(&mut conn)?;
                bad.require_kind(A)?;
                write(&mut conn, &bad.buf[..2])?;
                if retry {
                    let query = read(&mut conn)?;
                    answer(&mut conn, query, &[[192, 0, 2, 3]])?;
                }
            }
            Self::Close(retry) => {
                {
                    let mut conn = listen.accept().map_err(Server::Io)?.0;
                    read(&mut conn)?.require_kind(A)?;
                }
                if retry {
                    let mut conn = listen.accept().map_err(Server::Io)?.0;
                    let query = read(&mut conn)?;
                    answer(&mut conn, query, &[[192, 0, 2, 6]])?;
                }
            }
            Self::Partial => {
                let kind = {
                    let mut conn = listen.accept().map_err(Server::Io)?.0;
                    let done = read(&mut conn)?;
                    let lost = read(&mut conn)?;
                    let kind = lost.kind;
                    answer(&mut conn, done, &[[192, 0, 2, 7]])?;
                    kind
                };
                let mut conn = listen.accept().map_err(Server::Io)?.0;
                let retry = read(&mut conn)?;
                retry.require_kind(kind)?;
                answer(&mut conn, retry, &[[192, 0, 2, 7]])?;
            }
            Self::Cookie(Cookie::Missing | Cookie::Bad) => {
                let mut conn = listen.accept().map_err(Server::Io)?.0;
                let mut left = 2;
                while left != 0 {
                    let query = read(&mut conn)?;
                    answer(&mut conn, query, &[[192, 0, 2, 8]])?;
                    left -= 1;
                }
            }
            Self::Reply(_)
            | Self::Cookie(Cookie::Renew)
            | Self::Follow(_)
            | Self::Oversized
            | Self::Refused
            | Self::Reject(_)
            | Self::ExtendedCode(_)
            | Self::Delay(_) => {}
        }
        Ok(done)
    }
}

fn bind() -> io::Result<(SocketAddr, TcpListener, UdpSocket)> {
    for _ in 0..32 {
        let listen = TcpListener::bind("127.0.0.1:0")?;
        let addr = listen.local_addr()?;
        match UdpSocket::bind(addr) {
            Ok(udp) => return Ok((addr, listen, udp)),
            Err(error) if error.kind() == io::ErrorKind::AddrInUse => {}
            Err(error) => return Err(error),
        }
    }
    Err(io::ErrorKind::AddrInUse.into())
}

fn read(conn: &mut TcpStream) -> Result<Query<Vec<u8>>, Server> {
    conn.set_read_timeout(Some(LIMIT)).map_err(Server::Io)?;
    let mut head = [0; 2];
    conn.read_exact(&mut head).map_err(Server::Io)?;
    let mut buf = vec![0; usize::from(u16::from_be_bytes(head))];
    conn.read_exact(&mut buf).map_err(Server::Io)?;
    let query = Query::parse(buf)?;
    if query.buf[query.end - 2..query.end] != [0, 1]
        || query.buf[query.end..] != [0, 0, 41, 4, 208, 0, 0, 0, 0, 0, 0]
    {
        return Err(Server::Stream);
    }
    Ok(query)
}

fn write(conn: &mut TcpStream, buf: &[u8]) -> Result<(), Server> {
    conn.set_write_timeout(Some(LIMIT)).map_err(Server::Io)?;
    let len = u16::try_from(buf.len()).map_err(Server::Length)?;
    conn.write_all(&len.to_be_bytes()).map_err(Server::Io)?;
    conn.write_all(buf).map_err(Server::Io)
}

fn answer(conn: &mut TcpStream, query: Query<Vec<u8>>, vals: &[[u8; 4]]) -> Result<(), Server> {
    let reply = if query.kind == A {
        Reply::Addrs(vals, false, false)
    } else {
        Reply::Empty
    };
    write(conn, &query.reply(reply, false)?)
}
