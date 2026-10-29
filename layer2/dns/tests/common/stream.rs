use std::{
    cell::Cell,
    io::{self, Read as _, Write as _},
    iter,
    net::{SocketAddr, TcpListener, UdpSocket},
    num,
    os::fd::AsFd as _,
    pin,
    rc::Rc,
    task::Poll,
};

use dope::{
    core::{
        driver::{
            self,
            route::KeyTag,
            schedule::{Turn, clock, ready},
        },
        platform::{poll, thread, wake},
    },
    manifold::{
        app::{creation, dispatch},
        service::discover::{self, RefreshAt as _},
    },
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
use dope_test::harness::scenario::TIMEOUT;
use o3::alloc;

use crate::common::Owner;
use crate::common::refresh::{
    ADDRESSES, Advance, DATAGRAM_ROUTE, Failure, LANES, OUTBOUND, Resolution, Run, STREAM_ROUTE,
    server::{self, Stub},
};

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Packet,
    Source,
    Length(num::TryFromIntError),
    Create(wake::CreateError),
    Wait(wake::WaitError),
    Notify(wake::NotifyError),
    Peer,
}

pub struct Server {
    pub address: SocketAddr,
    pub other: SocketAddr,
    listener: TcpListener,
    udp: UdpSocket,
    fallback: TcpListener,
    socket: UdpSocket,
    waiting: wake::Notify,
    released: wake::Wait,
}

impl Server {
    pub fn new() -> Result<(Self, Signals), Error> {
        let listener = TcpListener::bind("127.0.0.1:0").map_err(Error::Io)?;
        let address = listener.local_addr().map_err(Error::Io)?;
        let udp = UdpSocket::bind(address).map_err(Error::Io)?;
        udp.set_read_timeout(Some(TIMEOUT)).map_err(Error::Io)?;
        let fallback = TcpListener::bind("127.0.0.1:0").map_err(Error::Io)?;
        let other = fallback.local_addr().map_err(Error::Io)?;
        fallback.set_nonblocking(true).map_err(Error::Io)?;
        let socket = UdpSocket::bind(other).map_err(Error::Io)?;
        socket.set_read_timeout(Some(TIMEOUT)).map_err(Error::Io)?;
        let (observed, waiting) = wake::Ends::blocking().map_err(Error::Create)?.split();
        let (released, release) = wake::Ends::blocking().map_err(Error::Create)?.split();
        Ok((
            Self {
                address,
                other,
                listener,
                udp,
                fallback,
                socket,
                waiting,
                released,
            },
            Signals {
                waiting: observed,
                release,
            },
        ))
    }
}

impl thread::Task for Server {
    type Output = Result<(), Error>;

    fn run(self) -> Self::Output {
        let Self {
            listener,
            udp,
            fallback: other,
            socket,
            waiting,
            released,
            ..
        } = self;
        let mut packet = [0; 512];
        for _ in 0..2 {
            let (len, source) = socket.recv_from(&mut packet).map_err(Error::Io)?;
            let mut reply = Stub::truncated(&packet[..len]).ok_or(Error::Packet)?;
            reply[2..4].copy_from_slice(&0x8185u16.to_be_bytes());
            socket.send_to(&reply, source).map_err(Error::Io)?;
        }
        let mut peer = None;
        for _ in 0..2 {
            let (len, source) = udp.recv_from(&mut packet).map_err(Error::Io)?;
            let reply = Stub::truncated(&packet[..len]).ok_or(Error::Packet)?;
            udp.send_to(&reply, source).map_err(Error::Io)?;
            peer = Some(source);
        }
        let mut stream = listener.accept().map_err(Error::Io)?.0;
        stream.set_read_timeout(Some(TIMEOUT)).map_err(Error::Io)?;
        let mut queries = Vec::new();
        for _ in 0..2 {
            let mut prefix = [0; 2];
            stream.read_exact(&mut prefix).map_err(Error::Io)?;
            let mut query = vec![0; usize::from(u16::from_be_bytes(prefix))];
            stream.read_exact(&mut query).map_err(Error::Io)?;
            queries.push(query);
        }
        waiting.notify().map_err(Error::Notify)?;
        udp.send_to(&[0], peer.ok_or(Error::Source)?)
            .map_err(Error::Io)?;
        released.wait_timeout(TIMEOUT).map_err(Error::Wait)?;
        for query in queries {
            let reply = Stub::answer(&query, 0, &[30], false).ok_or(Error::Packet)?;
            let length = u16::try_from(reply.len()).map_err(Error::Length)?;
            stream.write_all(&length.to_be_bytes()).map_err(Error::Io)?;
            stream.write_all(&reply).map_err(Error::Io)?;
        }
        match other.accept() {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(()),
            Err(error) => Err(Error::Io(error)),
            Ok(_) => Err(Error::Peer),
        }
    }
}

pub struct Signals {
    waiting: wake::Wait,
    release: wake::Notify,
}

impl Signals {
    pub fn tail<'report, 'a>(
        &'a self,
        finish: &'report mut session::Completion,
    ) -> Result<Tail<'report, 'a>, alloc::Error> {
        Ok(Tail {
            finish,
            waiting: &self.waiting,
            release: &self.release,
            events: poll::Set::try_new(iter::once(self.waiting.as_fd()), iter::empty())?,
        })
    }
}

pub struct Tail<'report, 'a> {
    finish: &'report mut session::Completion,
    waiting: &'a wake::Wait,
    release: &'a wake::Notify,
    events: poll::Set<'a>,
}

struct Observe<'a, 'd> {
    waiting: &'a wake::Wait,
    release: Option<&'a wake::Notify>,
    events: poll::Set<'a>,
    complete: Rc<Cell<u32>>,
    fault: Rc<Cell<Option<Advance>>>,
    root: Rc<Cell<Option<ready::Target<'d>>>>,
}

#[pin_project::pin_project]
#[derive(dope::Application)]
#[dispatcher(driver = 'd)]
struct App<'app, 's, 'd, 'signal, B> {
    #[pin]
    #[manifold]
    resolution: Resolution<'app, 's, 'd, B>,
    #[pin]
    #[manifold]
    observe: Observe<'signal, 'd>,
}

struct Until<'d> {
    complete: Rc<Cell<u32>>,
    fault: Rc<Cell<Option<Advance>>>,
    wake: Rc<Cell<Option<ready::Target<'d>>>>,
}

impl<'d> root::Root<'d> for Until<'d> {
    type Output = Result<(), Advance>;

    fn poll(context: root::Context<'_, 'd, Self>) -> Poll<Self::Output> {
        let (this, wake, _, _) = context.into_parts();
        let this = this.get_mut();
        this.wake.set(Some(wake));
        if let Some(error) = this.fault.take() {
            Poll::Ready(Err(error))
        } else if this.complete.get() != 0 {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

// SAFETY: the observer has no retained operations or pinned fields.
unsafe impl<'d> dispatch::raw::Manifold<'d> for Observe<'_, 'd> {
    const COORDS: usize = 0;

    type Route = KeyTag<5>;
    type Dispatch = dispatch::raw::Omit;
    type Install = dispatch::raw::Omit;
    type Finish = dispatch::raw::Omit;
    type Activate = dispatch::raw::Omit;
    type Probe = dispatch::raw::Omit;
    type PrePark = dispatch::raw::Plain;
    type Shutdown = dispatch::raw::Omit;

    unsafe fn pre_park<'a, 'turn, 'root>(
        self: pin::Pin<&'a mut Self>,
        _: Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'root, 'd, Self::PrePark>,
    ) -> pin::Pin<&'a mut Self>
    where
        Self: 'root,
    {
        let this = self.get_mut();
        if let Some(release) = this.release
            && !driver.driver_ref().ready().has_ready()
        {
            let ready = loop {
                match this.events.poll(0) {
                    Err(error) if error.is_intr() => {}
                    Ok(ready) => break ready.is_some(),
                    Err(error) => {
                        Advance::Signal(server::Failure::Poll(error)).record(&this.fault);
                        break false;
                    }
                }
            };
            if ready {
                let result = match this.waiting.wait() {
                    Ok(()) => release.notify().map_err(Advance::Notify),
                    Err(error) => Err(Advance::Wait(error)),
                };
                if let Err(error) = result {
                    error.record(&this.fault);
                }
                this.release = None;
            }
        }
        let fault = this.fault.take();
        let failed = fault.is_some();
        this.fault.set(fault);
        if (this.complete.get() != 0 || failed)
            && let Some(wake) = this.root.get()
        {
            wake.wake();
        }
        pin::Pin::new(this)
    }

    fn shutdown<'a, 'turn, 'root>(
        self: pin::Pin<&'a mut Self>,
        _: Turn<'turn, 'd>,
        _: &mut dispatch::raw::Context<'_, 'root, 'd, Self::Shutdown>,
    ) -> pin::Pin<&'a mut Self>
    where
        Self: 'root,
    {
        self
    }
}

struct Start<'a, 'd> {
    target: Target,
    now: clock::Moment<'d>,
    run: Run,
    waiting: &'a wake::Wait,
    release: &'a wake::Notify,
    events: poll::Set<'a>,
}

impl<'a, 'q, 'h, 'p, 'life, 's, 'd: 'life, R: Report + 'd>
    lending::Build<'d, Owner<'q, 'h, 'p, 'life, 's, 'd, R>> for Start<'a, 'd>
{
    type App<'app>
        = App<'app, 's, 'd, 'a, client::Bound<'app, 'd, Owner<'q, 'h, 'p, 'life, 's, 'd, R>>>
    where
        Self: 'app,
        Owner<'q, 'h, 'p, 'life, 's, 'd, R>: 'app,
        'd: 'app;
    type Data = Until<'d>;
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
        let until = Until {
            complete: self.run.changes.clone(),
            fault: self.run.fault.clone(),
            wake: Rc::new(Cell::new(None)),
        };
        let app = App {
            resolution: Resolution {
                discovery: query,
                run: self.run,
                next: Rc::new(Cell::new(Some(self.now.into()))),
            },
            observe: Observe {
                waiting: self.waiting,
                release: Some(self.release),
                events: self.events,
                complete: until.complete.clone(),
                fault: until.fault.clone(),
                root: until.wake.clone(),
            },
        };
        Ok((app, until))
    }
}

impl<'a, 'q, 'h, 'p, 'life, 's, 'd: 'life, R: Report + 'd>
    lending::Execute<'life, 'd, Owner<'q, 'h, 'p, 'life, 's, 'd, R>, ()> for Start<'a, 'd>
{
    type Output = Result<(), Advance>;

    fn run<'app, 'run>(
        data: Self::Data,
        mut app: session::App<
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
        app.drive_for(TIMEOUT, data).map_err(Advance::Drive)?
    }
}

pub fn compose<'report, 'scope, 'life: 'scope, 'e: 'life, 'd: 'e, R: Report + 'd>(
    tail: Tail<'report, '_>,
    install: Plan<KeyTag<DATAGRAM_ROUTE>, KeyTag<STREAM_ROUTE>, OUTBOUND, LANES, ADDRESSES, R>,
    stage: &mut lending::Stage<'_, 'scope, 'life, 'e, 'd, storage::Plan<LANES, ADDRESSES>, ()>,
    outcome: &mut Option<Result<Result<(), Failure<'report>>, creation::Error>>,
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
                    *outcome = Some(Ok(Err(Failure::Target(error))));
                    return;
                }
            };
            let run = Run::new(false, 1);
            let fault = run.fault.clone();
            let build = Start {
                target,
                now: stage.driver().driver_ref().clock().now(),
                run,
                waiting: tail.waiting,
                release: tail.release,
                events: tail.events,
            };
            *outcome = Some(Ok(
                match stage.lend_for(tail.finish, owner, build, TIMEOUT) {
                    Ok(Ok(())) => match fault.take() {
                        Some(error) => Err(Failure::Advance(error)),
                        None => Ok(()),
                    },
                    Ok(Err(error)) => Err(Failure::Advance(error)),
                    Err(error) => Err(Failure::from(error)),
                },
            ));
        },
    );
    if let Err(error) = installed {
        *outcome = Some(Err(error));
    }
}
