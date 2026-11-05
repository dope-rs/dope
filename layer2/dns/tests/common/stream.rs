use std::{cell::Cell, mem, net::SocketAddr, rc::Rc, task::Poll};

use dope::{
    core::driver::{self, route::KeyTag, schedule::clock},
    manifold::service::discover::{self, RefreshAt as _},
    runtime::{
        client,
        executor::{root, session},
        topology::lending,
    },
};
use dope_dns::{
    Report, Target,
    lookup::{create, refresh},
    storage,
    topology::Plan,
};
use dope_test::harness::{
    peer::{self, Stream, TcpAcceptor, udp},
    scenario::TIMEOUT,
};

use crate::common::{
    Owner,
    refresh::{
        ADDRESSES, Advance, App, DATAGRAM_ROUTE, Failure, LANES, OUTBOUND, Resolution, Run,
        STREAM_ROUTE,
        server::{Failure as PeerFailure, Stub},
    },
};

#[derive(Clone, Copy)]
enum Stage {
    Datagrams(u8),
    Queries(SocketAddr, bool),
    Waiting,
    Replies(bool),
    Done,
}

pub struct Server {
    pub address: SocketAddr,
    pub other: SocketAddr,
    listener: TcpAcceptor<'static>,
    udp: udp::Socket<'static>,
    fallback: TcpAcceptor<'static>,
    socket: udp::Socket<'static>,
    stream: Option<Connection>,
    queries: [Vec<u8>; 2],
    stage: Stage,
}

impl Server {
    pub fn new() -> Result<Self, PeerFailure> {
        let (address, listener, udp) = crate::common::socket::bind().map_err(PeerFailure::Io)?;
        let (other, fallback, socket) = crate::common::socket::bind().map_err(PeerFailure::Io)?;
        let udp = udp::Socket::try_adopt(udp).map_err(PeerFailure::Io)?;
        let socket = udp::Socket::try_adopt(socket).map_err(PeerFailure::Io)?;
        udp.nonblock(true);
        socket.nonblock(true);
        Ok(Self {
            address,
            other,
            listener: TcpAcceptor::try_adopt(listener).map_err(PeerFailure::Io)?,
            udp,
            fallback: TcpAcceptor::try_adopt(fallback).map_err(PeerFailure::Io)?,
            socket,
            stream: None,
            queries: [Vec::new(), Vec::new()],
            stage: Stage::Datagrams(0),
        })
    }

    fn poll(&mut self) -> Result<bool, PeerFailure> {
        match self.stage {
            Stage::Datagrams(round) => {
                let socket = if round < 2 { &self.socket } else { &self.udp };
                let mut packet = [0; 512];
                let (len, peer) = match socket.recv_from(&mut packet) {
                    Ok(value) => value,
                    Err(peer::Error::Syscall(error)) if error.is_block() || error.is_intr() => {
                        return Ok(false);
                    }
                    Err(error) => return Err(PeerFailure::Io(error)),
                };
                let mut reply = Stub::truncated(&packet[..len]).ok_or(PeerFailure::Packet)?;
                if round < 2 {
                    reply[2..4].copy_from_slice(&0x8185u16.to_be_bytes());
                }
                socket.send_to(&reply, peer).map_err(PeerFailure::Io)?;
                self.stage = if round == 3 {
                    Stage::Queries(peer, false)
                } else {
                    Stage::Datagrams(round + 1)
                };
                return Ok(false);
            }
            Stage::Waiting => return Ok(true),
            Stage::Done => return Ok(false),
            Stage::Queries(_, _) | Stage::Replies(_) => {}
        }
        if self.stream.is_none() {
            let Some(stream) = self.listener.try_accept().map_err(PeerFailure::Io)? else {
                return Ok(false);
            };
            self.stream = Some(Connection::new(stream));
        }
        let Some(stream) = &mut self.stream else {
            unreachable!("stream")
        };
        match stream.poll()? {
            Poll::Pending => {}
            Poll::Ready(Event::Closed(error)) => return Err(PeerFailure::Io(error)),
            Poll::Ready(Event::Query(query)) => {
                let Stage::Queries(peer, second) = self.stage else {
                    unreachable!("query")
                };
                self.queries[usize::from(second)] = query;
                if second {
                    self.udp.send_to(&[0], peer).map_err(PeerFailure::Io)?;
                    self.stage = Stage::Waiting;
                    return Ok(true);
                }
                self.stage = Stage::Queries(peer, true);
            }
            Poll::Ready(Event::Written) => {
                let Stage::Replies(second) = self.stage else {
                    unreachable!("reply")
                };
                self.stage = if second {
                    Stage::Done
                } else {
                    stream.reply(mem::take(&mut self.queries[1]))?;
                    Stage::Replies(true)
                };
            }
        }
        Ok(false)
    }
}

pub struct Tail<'report> {
    pub finish: &'report mut session::Completion,
    pub server: Server,
}

struct Until<'a> {
    server: &'a mut Server,
    run: &'a Run,
}

impl<'d> root::Root<'d> for Until<'_> {
    type Output = Result<bool, Advance>;

    fn poll(context: root::Context<'_, 'd, Self>) -> Poll<Self::Output> {
        let (this, wake, _, _) = context.into_parts();
        let this = this.get_mut();
        if let Some(error) = this.run.fault.take() {
            return Poll::Ready(Err(error));
        }
        if this.run.changes.get() != 0 {
            return Poll::Ready(if matches!(this.server.stage, Stage::Done) {
                Ok(true)
            } else {
                Err(Advance::Signal(PeerFailure::Phase))
            });
        }
        match this.server.poll() {
            Ok(true) => Poll::Ready(Ok(false)),
            Err(error) => Poll::Ready(Err(Advance::Signal(error))),
            Ok(false) => {
                wake.wake();
                Poll::Pending
            }
        }
    }
}

struct Start<'d> {
    target: Target,
    now: clock::Moment<'d>,
    run: Run,
    server: Server,
}

impl<'q, 'h, 'p, 'life, 's, 'd: 'life, R: Report + 'd>
    lending::Build<'d, Owner<'q, 'h, 'p, 'life, 's, 'd, R>> for Start<'d>
{
    type App<'app>
        = App<'app, 's, 'd, client::Bound<'app, 'd, Owner<'q, 'h, 'p, 'life, 's, 'd, R>>>
    where
        Self: 'app,
        Owner<'q, 'h, 'p, 'life, 's, 'd, R>: 'app,
        'd: 'app;
    type Data = (Server, Run, clock::Deadline<'d>);
    type Error = create::Error;

    fn build<'app>(
        self,
        resolver: <Owner<'q, 'h, 'p, 'life, 's, 'd, R> as client::Provider<'d>>::Client<'app>,
        mut driver: driver::Context<'_, 'd>,
    ) -> Result<(Self::App<'app>, Self::Data), Self::Error>
    where
        Self: 'app,
        Owner<'q, 'h, 'p, 'life, 's, 'd, R>: 'app,
        'd: 'app,
    {
        let mut query =
            resolver.discovery::<refresh::Transport>(driver.state_token(), self.target)?;
        query.refresh_at(self.now, discover::Reason::Startup);
        let app = App {
            resolution: Resolution {
                discovery: query,
                run: self.run.clone(),
                next: Rc::new(Cell::new(Some(self.now.into()))),
            },
        };
        Ok((app, (self.server, self.run, self.now.into())))
    }
}

impl<'q, 'h, 'p, 'life, 's, 'd: 'life, R: Report + 'd>
    lending::ExecuteAt<'life, 'd, Owner<'q, 'h, 'p, 'life, 's, 'd, R>, ()> for Start<'d>
{
    type Output = Result<(), Advance>;

    fn run<'app, 'run>(
        (mut server, run, now): Self::Data,
        mut app: session::Manual<
            'run,
            'life,
            'd,
            lending::raw::Bound<'app, 'd, Owner<'q, 'h, 'p, 'life, 's, 'd, R>, Self::App<'app>>,
            (),
        >,
    ) -> Self::Output
    where
        Self: 'app,
        Owner<'q, 'h, 'p, 'life, 's, 'd, R>: 'app,
        'd: 'app,
        'app: 'run,
        'life: 'run,
    {
        loop {
            let done = app
                .drive_for(
                    now,
                    TIMEOUT,
                    Until {
                        server: &mut server,
                        run: &run,
                    },
                )
                .map_err(Advance::Drive)?
                .into_output()?;
            if let Some(error) = run.fault.take() {
                return Err(error);
            }
            if done {
                break;
            }
            let Some(stream) = &mut server.stream else {
                unreachable!("stream")
            };
            stream
                .reply(mem::take(&mut server.queries[0]))
                .map_err(Advance::Signal)?;
            server.stage = Stage::Replies(false);
        }
        match server
            .fallback
            .try_accept()
            .map_err(PeerFailure::Io)
            .map_err(Advance::Signal)?
        {
            None => Ok(()),
            Some(_) => Err(Advance::Signal(PeerFailure::Peer)),
        }
    }
}

pub fn compose<'report, 'scope, 'life: 'scope, 'e: 'life, 'd: 'e, R: Report + 'd>(
    tail: Tail<'report>,
    install: Plan<KeyTag<DATAGRAM_ROUTE>, KeyTag<STREAM_ROUTE>, OUTBOUND, LANES, ADDRESSES, R>,
    stage: &mut lending::Stage<'_, 'scope, 'life, 'e, 'd, storage::Plan<LANES, ADDRESSES>, ()>,
    outcome: &mut Option<Result<(), Failure<'report>>>,
) {
    let installed = lending::Installer::install(
        install,
        stage,
        |owner: Owner<'_, '_, '_, 'life, 'e, 'd, R>,
         stage: &mut lending::Stage<
            '_,
            'scope,
            'life,
            'e,
            'd,
            storage::Plan<LANES, ADDRESSES>,
            (),
        >| {
            let target = match Target::parse("service.example:443") {
                Ok(target) => target,
                Err(error) => {
                    *outcome = Some(Err(Failure::Target(error)));
                    return;
                }
            };
            let run = Run::new(false, 1);
            let fault = run.fault.clone();
            let now = stage.driver().driver_ref().clock().now();
            let build = Start {
                target,
                now,
                run,
                server: tail.server,
            };
            *outcome = Some(
                match stage.lend_at(tail.finish, owner, build, now.into(), TIMEOUT) {
                    Ok(Ok(())) => match fault.take() {
                        Some(error) => Err(Failure::Advance(error)),
                        None => Ok(()),
                    },
                    Ok(Err(error)) => Err(Failure::Advance(error)),
                    Err(error) => Err(Failure::from(error)),
                },
            );
        },
    );
    if let Err(error) = installed {
        *outcome = Some(Err(Failure::Install(error)));
    }
}

enum Frame {
    Prefix([u8; 2], usize),
    Query(Vec<u8>, usize),
    Reply([u8; 2], Vec<u8>, usize),
}

pub(super) enum Event {
    Query(Vec<u8>),
    Written,
    Closed(peer::Error),
}

pub(super) struct Connection {
    stream: Stream<'static>,
    frame: Frame,
}

impl Connection {
    pub(super) fn new(stream: Stream<'static>) -> Self {
        stream.nonblock(true);
        Self {
            stream,
            frame: Frame::Prefix([0; 2], 0),
        }
    }

    pub(super) fn reply(&mut self, query: Vec<u8>) -> Result<(), PeerFailure> {
        let reply = Stub::answer(&query, 0, &[30], false).ok_or(PeerFailure::Packet)?;
        let len = u16::try_from(reply.len()).map_err(PeerFailure::Length)?;
        self.frame = Frame::Reply(len.to_be_bytes(), reply, 0);
        Ok(())
    }

    pub(super) fn poll(&mut self) -> Result<Poll<Event>, PeerFailure> {
        match self.advance() {
            Err(PeerFailure::Io(peer::Error::Syscall(error)))
                if error.is_block() || error.is_intr() =>
            {
                Ok(Poll::Pending)
            }
            result => result,
        }
    }

    fn advance(&mut self) -> Result<Poll<Event>, PeerFailure> {
        match &mut self.frame {
            Frame::Prefix(prefix, read) => {
                let count = self
                    .stream
                    .read(&mut prefix[*read..])
                    .map_err(PeerFailure::Io)?;
                if count == 0 {
                    return Ok(Poll::Ready(Event::Closed(peer::Error::UnexpectedEof)));
                }
                *read += count;
                if *read == prefix.len() {
                    let len = usize::from(u16::from_be_bytes(*prefix));
                    if len == 0 {
                        return Err(PeerFailure::Packet);
                    }
                    self.frame = Frame::Query(vec![0; len], 0);
                }
            }
            Frame::Query(query, read) => {
                let count = self
                    .stream
                    .read(&mut query[*read..])
                    .map_err(PeerFailure::Io)?;
                if count == 0 {
                    return Err(PeerFailure::Io(peer::Error::UnexpectedEof));
                }
                *read += count;
                if *read == query.len() {
                    let query = mem::take(query);
                    self.frame = Frame::Prefix([0; 2], 0);
                    return Ok(Poll::Ready(Event::Query(query)));
                }
            }
            Frame::Reply(prefix, reply, sent) => {
                let write = if *sent < prefix.len() {
                    self.stream.write_vectored(&[&prefix[*sent..], reply])
                } else {
                    self.stream.write(&reply[*sent - prefix.len()..])
                };
                let count = match write {
                    Ok(0) => return Err(PeerFailure::Io(peer::Error::WriteZero)),
                    Ok(count) => count,
                    Err(peer::Error::Syscall(error))
                        if error.is_pipe()
                            || error.is_aborted()
                            || error.is_reset()
                            || error.is_not_connected() =>
                    {
                        return Ok(Poll::Ready(Event::Closed(peer::Error::Syscall(error))));
                    }
                    Err(error) => return Err(PeerFailure::Io(error)),
                };
                *sent += count;
                if *sent == prefix.len() + reply.len() {
                    self.frame = Frame::Prefix([0; 2], 0);
                    return Ok(Poll::Ready(Event::Written));
                }
            }
        }
        Ok(Poll::Pending)
    }
}
