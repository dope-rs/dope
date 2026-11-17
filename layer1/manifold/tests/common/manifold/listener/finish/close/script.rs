use o3::buffer::write::Sink as _;
use std::{convert, net, pin::Pin, time};

use dope_core::{
    driver::{self, route::Identity, schedule::clock},
    random::Seed,
};
use dope_manifold::{
    app::{receive, receive::Outcome},
    network::listener::{
        conn::{self, write},
        handler::{self, Application, failures, retire, send},
    },
};
use dope_test::harness::{peer, peer::Peer, signals::Gate};
use o3::buffer::{bytes::TryRetain, resident};

use crate::common::manifold::listener::finish::close::wire::Wire;

pub struct App {
    pub(crate) payload: Option<Vec<u8>>,
    pub(crate) close: Gate,
    pub(crate) chunk: Option<Gate>,
    pub(crate) timeout: Option<conn::deadline::Kind>,
    pub(crate) abandon: bool,
}

#[derive(Debug)]
pub enum Read {
    Connect(peer::Error),
    Write(peer::Error),
    Receive(peer::Error),
}

pub struct Script(pub(crate) net::SocketAddr);
impl Script {
    pub fn try_read(self) -> Result<Vec<u8>, Read> {
        let stream = Peer::at(self.0)
            .try_read(time::Duration::from_secs(10))
            .map_err(Read::Connect)?;
        stream.write_all(b"REQ").map_err(Read::Write)?;
        Peer::try_read_all(&stream).map_err(Read::Receive)
    }
}

impl<'d, ID: Identity> Application<'d, ID> for App {
    type Conn = u8;
    type Build = convert::Infallible;
    type Wire = Wire;
    type WireFailure = failures::Close;
    type Retirement = ();
    type Egress = resident::Shared<'d>;
    type Input = receive::Borrowed;
    type Send = send::Silent;

    fn build_failed(self: Pin<&mut Self>, cause: Self::Build) {
        match cause {}
    }

    fn connection(self: Pin<&Self>, _: &Seed) -> Result<Self::Conn, Self::Build> {
        Ok(0)
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn timing(&self) -> handler::Timing {
        if self.abandon {
            handler::Timing::timeout(time::Duration::from_millis(50))
        } else {
            handler::Timing::default()
        }
    }

    fn timeout(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire, u8>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> handler::Timeout<'d> {
        assert!(self.abandon);
        assert!(matches!(
            conn.write().pending(),
            dope_net::link::egress::Pending::Present
        ));
        let before = conn.work().remaining();
        for _ in 0..2 {
            let mut output_slot = std::mem::MaybeUninit::uninit();
            let mut output = conn.write().try_prefix(driver, &mut output_slot).expect("queued admission");
            output.get_mut().parts().1.write_byte(b'X').unwrap();
            drop(output);
            assert_eq!(conn.work().remaining(), before);
        }
        self.chunk.as_ref().expect("abandonment receipt").hit();
        handler::Timeout::CloseAfter
    }

    fn drain<'e>(
        self: Pin<&mut Self>,
        _: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Wire, u8>,
        _: &mut driver::Context<'_, 'd>,
    ) -> handler::Drain {
        handler::Drain::Complete
    }

    fn close<'e>(self: Pin<&mut Self>, conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Wire, u8>) {
        assert!(*conn.state() <= 1);
        self.as_ref().get_ref().close.hit();
    }
}

impl<'d, ID: Identity> retire::Policy<'d, App, ID> for () {
    fn select() -> Option<Self> {
        Some(())
    }

    fn retire(self, app: Pin<&mut App>, conn: conn::Retired<'_, '_, 'd, ID, Wire, u8>) {
        if let Some(expected) = app.timeout {
            assert!(
                matches!(conn.failure(), Some(conn::Failure::Timeout(kind)) if *kind == expected),
                "missing {expected:?} expiry"
            );
        }
    }
}

impl<'d, ID: Identity> handler::Borrowed<'d, ID> for App {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire, u8>,
        chunk: R,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Outcome {
        assert!(!chunk.as_ref().is_empty());
        if let Some(gate) = self.as_ref().get_ref().chunk.as_ref() {
            gate.hit();
        }
        let Some(reply) = self.as_ref().get_ref().payload.as_ref() else {
            return Outcome::Continue;
        };
        let mut output_slot = std::mem::MaybeUninit::uninit();
        let mut output = conn.write().try_prefix(driver, &mut output_slot).expect("prefix admission");
        let (state, mut prefix) = output.get_mut().parts();
        assert_eq!(*state, 0);
        *state = 1;
        prefix.write_slice(reply).unwrap();
        write::Output::finish(output, write::End::Open)
            .map_err(write::error::Mismatch::into_cause)
            .expect("prefix publication");
        if self.abandon {
            conn.deadline()
                .try_ensure(driver.driver_ref().clock().now())
                .expect("abandonment deadline");
            Outcome::Continue
        } else {
            Outcome::CloseAfter
        }
    }
}

pub fn deadline(draining: bool, abandon: bool) {
    use crate::common::{recover, scenario::Runtime};
    use dope_core::driver::{
        life::recovery,
        route::{FrameworkTag, KeyTag, Tag, kind},
    };
    use dope_manifold::network::listener::conn::deadline::Kind;
    use dope_runtime::executor::session::{self, Completion};
    use dope_test::{harness::signals::Progress as _, reclaim::Reports};
    use std::time::Duration;

    use crate::common::manifold::listener::defer::{script::Until, wire::Profile};
    use dope_core::{
        driver::{
            route::table::conn::Capacity,
            settings::{Resource as _, claim::Needs},
        },
        io::socket,
    };
    use dope_manifold::{
        app::{Host, env::Bundle, receive},
        network::listener::{self, config, handler, topology},
    };
    use dope_net::{link::egress, tcp::Tcp, wire::domain};
    use o3::base::time::Positive;

    let mut finish = Completion::new();
    let entered = Gate::new();
    let release = Gate::new();
    let received = Gate::new();
    let closed = Gate::new();
    let layout = topology::Layout::<KeyTag<0>, receive::Borrowed>::new(Capacity::fixed::<1>());
    let settings = layout
        .include_in(Needs::new())
        .expect("listener resources")
        .resolve::<Profile>()
        .expect("listener settings");
    let mut reports = Reports::<_>::new(settings, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    Runtime::new(settings).with_scope(
        |mut session| {
            let config = config::Prepared {
                connections: Capacity::fixed::<1>(),
                per_ip: None,
                direct_flights: usize::from(!abandon).into(),
                bind: ([127, 0, 0, 1], 0).into(),
                backlog: socket::Backlog::fixed::<1>(),
                stream: Default::default(),
                transport: Default::default(),
                egress: egress::Config::shared(if abandon {
                    egress::Credit::fixed::<2, 32768>()
                } else {
                    egress::Credit::fixed::<0, 0>()
                }),
            };
            let hash = session.hash_state(listener::Domain::DEFAULT);
            let wire = Wire::waiting(entered.clone(), release.clone());
            let install = listener::Install::<KeyTag<0>, Bundle<Tcp, Wire, Profile>, _>::new(
                config,
                domain::Local::new(wire),
            );
            let install = if draining {
                install.with_life(handler::Lifetime::graceful(
                    Positive::from_millis::<20>(),
                    Positive::from_millis::<20>(),
                ))
            } else {
                install
            };
            let result = install.with_driver(
                App {
                    payload: (!draining).then(|| b"R".to_vec()),
                    close: closed.clone(),
                    chunk: Some(received.clone()),
                    timeout: Some(if draining { Kind::Drain } else { Kind::Send }),
                    abandon,
                },
                hash,
                &mut session,
                session::Scope::driver,
                |server, _wait, session| {
                    let addr = server.local_addr();
                    let start = session.driver().driver_ref().clock().now().into();
                    let bounded = session
                        .with_at_for(
                            &mut finish,
                            Host::new(server),
                            start,
                            Duration::from_secs(2),
                            |mut app| {
                                let peer = Peer::at(addr).try_connect().expect("connect peer");
                                peer.write_all(b"REQ").expect("write request");
                                app.drive_for(
                                    start,
                                    Duration::from_secs(1),
                                    Until {
                                        gate: if abandon { &entered } else { &received },
                                        hits: 1,
                                    },
                                )
                                .expect("initial listener progress")
                                .into_output();
                                let begin = start
                                    .try_add(Duration::from_millis(if abandon {
                                        50
                                    } else if draining {
                                        21
                                    } else {
                                        0
                                    }))
                                    .expect("close coordinate");
                                app.drive_for(
                                    begin,
                                    Duration::from_secs(1),
                                    Until {
                                        gate: if abandon { &received } else { &entered },
                                        hits: if abandon { 2 } else { 1 },
                                    },
                                )
                                .expect("close waits for wire storage")
                                .into_output();
                                assert_eq!(closed.hits(), 0);
                                let due = start
                                    .try_add(Duration::from_millis(if draining { 42 } else { 101 }))
                                    .expect("deadline coordinate");
                                let bounded = app
                                    .drive_for(
                                        due,
                                        Duration::from_millis(200),
                                        Until {
                                            gate: &closed,
                                            hits: 1,
                                        },
                                    )
                                    .is_ok();
                                assert_eq!(release.hits(), 0);
                                release.hit();
                                bounded
                            },
                        )
                        .expect("listener teardown");
                    assert!(bounded, "close wait lost its deadline; draining={draining}");
                },
            );
            assert!(result.is_ok(), "listener installation");
        },
        recovery::Policy::new(|event| reports.push(event), recover),
    );
    assert!(reports.events().expect("reclaimed events").is_empty());
}
