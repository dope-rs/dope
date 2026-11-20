use std::{convert::Infallible, net::UdpSocket, num::NonZero, ops::ControlFlow, time::Duration};

use dope::{
    core::{
        driver::{life, life::recovery, route::table::conn, settings},
        io::{
            event::{self, net::recv},
            fd::handles::Descriptor,
            socket::{install::Install, option},
        },
    },
    net::{
        link::{
            egress::{self, reserve::demand},
            event::{ControlRecv, Dispatch, DispatchRecv, Reservation},
            pool::{
                change::{self, accept::Outcome},
                recv::input,
            },
        },
        tcp::Tcp,
        wire::{
            domain,
            receive::{Chunk, mode::Disabled},
        },
    },
};
use dope_test::scope::Driver;
use dope_tls::tls::endpoint::layout::Staged;

use super::{Error, Key, Pool, Route, Wire, native, settle, submit};
use crate::common::{self, fiber::Peer, native as tls, state};

type Received<'w, 'd> =
    Reservation<'d, Route, DispatchRecv<'d, Route, Chunk<'w, 'd, Wire, Disabled>>>;

pub struct Records<'d> {
    pub key: Key<'d>,
    peer: UdpSocket,
    tls: rustls::Connection,
}

impl<'d> Records<'d> {
    fn open(
        pool: &mut Pool<'_, '_, 'd>,
        scope: &mut life::Scope<'_, 'd>,
        pubkey: [u8; 32],
    ) -> Result<Self, Error> {
        // Each datagram delivers one chosen ciphertext fragment in one native receive lease.
        let server = UdpSocket::bind(("127.0.0.1", 0)).expect("bind receiver");
        let peer = UdpSocket::bind(("127.0.0.1", 0)).expect("bind peer");
        server
            .connect(peer.local_addr().expect("peer address"))
            .expect("connect receiver");
        peer.connect(server.local_addr().expect("receiver address"))
            .expect("connect peer");
        peer.set_read_timeout(Some(Duration::from_secs(5)))
            .expect("peer deadline");
        let (socket, _) =
            Install::adopt_udp(scope.context(), server.into()).expect("install receiver");
        let fd = Descriptor::from(socket);
        let ready = fd.ready_handle();
        let opened = pool
            .accept_with(
                conn::Capacity::fixed::<1>(),
                fd,
                option::Set::EMPTY,
                |_| Ok::<_, Infallible>(()),
                &mut scope.context(),
            )
            .map_err(|error| Error::Open(error.into_cause()))?;
        let key = match opened {
            Outcome::Ready(key) => key,
            Outcome::Failed(_, cause) => return Err(Error::Tuning(cause)),
            _ => return Err(Error::Opened),
        };
        ready.set_target(key.target().dispatch());
        pool.recv()
            .arm(key, &mut scope.context())
            .map_err(|error| Error::Submit(error.cause))?;
        let mut records = Self {
            key,
            peer,
            tls: Peer::client(pubkey).map_err(Error::Connect)?,
        };
        for _ in 0..32 {
            let wire = tls::output(&mut records.tls).map_err(Error::Connect)?;
            if !wire.is_empty() {
                let (received, _) = records.receive(pool, scope, &wire)?;
                if !matches!(received,
                    Reservation::Ready(DispatchRecv::Control(ControlRecv::NoChunk(k))) if k == key
                ) {
                    return Err(Error::Handshake);
                }
            }
            submit(pool, key, scope)?;
            if pool.inspect().get(key).ok_or(Error::Key)?.send_status().is_inflight() {
                let done = native::next_send(scope)?;
                let sent = done.outcome();
                assert_eq!(settle(pool, done, scope)?, key);
                if let Ok(count) = sent && !count.is_zero() {
                    let mut bytes = [0; 16 * 1024];
                    let read = records.peer.recv(&mut bytes).expect("receive handshake flight");
                    assert_eq!(read, count.into_raw());
                    tls::input(&mut records.tls, &bytes[..read]).map_err(Error::Connect)?;
                }
            } else if !records.tls.is_handshaking() && !records.tls.wants_write() {
                return Ok(records);
            }
        }
        Err(Error::Handshake)
    }

    pub fn seal(&mut self, body: &[u8]) -> Vec<u8> {
        tls::seal(&mut self.tls, body).expect("seal one TLS record")
    }

    pub fn receive<'w>(
        &self,
        pool: &mut Pool<'_, 'w, 'd>,
        scope: &mut life::Scope<'_, 'd>,
        wire: &[u8],
    ) -> Result<(Received<'w, 'd>, *const u8), Error>
    where
        'd: 'w,
    {
        assert_eq!(self.peer.send(wire).expect("send ciphertext"), wire.len());
        for _ in 0..8 {
            flush(pool, scope)?;
            let event::Kind::Recv(done) = native::next(scope)?.into_kind() else {
                return Err(Error::RecvEvent("expected data"));
            };
            let mut address = None;
            let done = match done.dispatch(|_, _, outcome| {
                if let recv::Outcome::Data(lease) = &outcome {
                    assert_eq!(lease.as_slice(), wire);
                    address = Some(lease.as_slice().as_ptr());
                }
                ControlFlow::<_, Infallible>::Break(outcome)
            }) {
                ControlFlow::Break(done) => done,
                ControlFlow::Continue(never) => match never {},
            };
            match done.classify() {
                recv::Kind::Data(data) => {
                    let (_, result) =
                        pool.recv().data(data, |_, _| (), |plan| plan.map(|data| data.scoped()));
                    return Ok((result, address.expect("provided input address")));
                }
                recv::Kind::Control(control) => match pool.recv().dispatch(control).1 {
                    Dispatch::Settled => {}
                    Dispatch::Ready(ControlRecv::Failed { error, .. }) => {
                        return Err(Error::Receive(error));
                    }
                    Dispatch::Ready(ControlRecv::Credit { cause, .. }) => {
                        return Err(Error::Credit(cause));
                    }
                    _ => return Err(Error::Control),
                },
            }
        }
        Err(Error::RecvEvent("control limit"))
    }
}

pub fn flush<'d>(
    pool: &mut Pool<'_, '_, 'd>,
    scope: &mut life::Scope<'_, 'd>,
) -> Result<(), Error> {
    let (_, mut driver, mut control) = scope.split();
    pool.recv()
        .flush(control.begin().turn().maint(), &mut driver)
        .map_err(|error| Error::Submit(error.cause))
}

pub fn run(
    test: impl for<'p, 'w, 'd> FnOnce(
        &mut Records<'d>,
        &mut Pool<'p, 'w, 'd>,
        &mut life::Scope<'_, 'd>,
    ) -> Result<(), Error>,
) {
    let (end, pubkey) = Peer::try_server().expect("server identity");
    let size = conn::Capacity::fixed::<1>();
    let slots = domain::Owner::<Wire, Route>::try_new(size).expect("TLS endpoint storage");
    let config = settings::Config::default()
        .with_queues(state::QUEUES)
        .with_receive(settings::Receive::fixed::<8, 4096>())
        .with_domains(settings::Domain::ONE);
    Driver::new(config).with_scope(
        |scope| {
            let Ok(end) =
                slots.bind(end.with_staged(Staged::fixed::<0>()).with_growth(NonZero::<u32>::MIN));
            change::try_prepare::<Route, Tcp, Wire, (), input::Scoped, &[u8], 32, _, _, _, _>(
                domain::Retained::ZERO,
                domain::Resident::ZERO,
                demand::Fallible::prepare(egress::Config::DEFAULT, size),
                end,
                scope,
                life::Scope::context,
                |(prepared, need), scope| {
                    let mut driver = scope.context();
                    let flights =
                        driver.storage.reborrow().try_flights(need).expect("TLS flights").slots();
                    let route =
                        driver.storage.reborrow().try_reserve::<Route>().expect("receive route");
                    // SAFETY: the pool remains in this scope until all submitted native I/O is retired.
                    let mut pool = unsafe { prepared.commit(flights).bind(route) };
                    let result = Records::open(&mut pool, scope, pubkey)
                        .and_then(|mut records| test(&mut records, &mut pool, scope));
                    scope.finish(true, |_, finish| pool.finish(finish, |_, _| {}));
                    result
                },
            )
            .expect("prepare TLS pool")
        },
        recovery::Policy::new(|_| {}, common::recover),
        |result| {
            result
                .as_ref()
                .map_err(|error| error.cause())
                .expect("driver scope")
                .as_ref()
                .expect("TLS receive");
        },
    );
}
