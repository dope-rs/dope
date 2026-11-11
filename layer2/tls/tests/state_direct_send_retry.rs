pub mod common;

use std::{cell::Cell, num::NonZero, sync::Arc, time::Duration};

use dope::{
    core::{
        driver::{
            life::{creation::Attempt, recovery},
            route::{FrameworkTag, KeyTag, Tag, kind},
            settings::{self, claim::Needs},
            storage as driver_storage,
        },
        io::socket,
    },
    manifold::{
        app::{Host, env::Bundle, receive::Borrowed},
        network::listener::{self, config::Config, conn::write, handler, topology::Layout},
        timing,
    },
    net::{link::egress, tcp::Tcp, wire::domain::Owner},
    runtime::executor::{Executor, drive, session, session::Completion},
};
use dope_test::{
    fibers,
    harness::{
        GUARD, peer,
        scenario::TIMEOUT,
        signals::{Gate, Progress as _},
    },
    reclaim::Reports,
};
use dope_tls::tls::{Tls, endpoint, roles::Server};
use o3::buffer::storage;

use common::{
    reply::{Capacity, Change, Event, Mode, PREFIX, Reply, run_pair},
    state::{QUEUES, signing_key},
};

type Wire = Tls<Server>;

#[test]
fn released_direct() {
    for forced in [false, true] {
        let key = signing_key().expect("signing key");
        let pubkey = key.public_key();
        let body = b"direct reply after ciphertext release";
        let ready = Gate::new();
        let closes = Gate::new();
        let inputs = Cell::new(0);
        let writes = Cell::new(0);
        let started = Gate::new();
        let released = Cell::new([0; 2]);
        let discarded = Cell::new(0);
        let observe = |event: Event<'_>| match event {
            Event::Open(cause) => panic!("TLS open: {cause:?}"),
            Event::Wire(cause) => panic!("TLS wire: {cause:?}"),
            Event::Peer(cause) => panic!("TLS peer: {cause:?}"),
            Event::Copy(cause) => panic!("reply allocation: {cause:?}"),
            Event::Input(bytes) => {
                assert_eq!(bytes, b"start");
                inputs.set(inputs.get() + 1);
            }
            Event::Eof(_) => panic!("unexpected peer EOF"),
            Event::Write(result) => {
                assert!(matches!(result, Ok(write::Outcome::Committed(()))));
                writes.set(writes.get() + 1);
            }
        };
        let net = Config::<Tcp> {
            max_connections: 2,
            per_ip: None,
            direct_flights: if forced { 0.into() } else { 2.into() },
            bind: ([127, 0, 0, 1], 0).into(),
            backlog: socket::Backlog::fixed::<2>(),
            stream: Default::default(),
            transport: Default::default(),
            egress: if forced {
                egress::Config::shared(egress::Credit::fixed::<4, 65536>())
            } else {
                egress::Config::shared(egress::Credit::fixed::<0, 0>())
            },
        }
        .try_prepare()
        .expect("listener capacity");
        let layout = Layout::<KeyTag<0>, Borrowed>::new(net.connections());
        let drive = Needs::from_tree(&layout)
            .and_then(|needs| needs.resolve::<timing::Throughput>())
            .expect("driver configuration");
        let mut attempt = Attempt::new();
        let exec = Executor::new(
            &mut attempt,
            drive
                .with_queues(QUEUES)
                .with_domains(settings::Domain::ONE),
        )
        .map_err(|cause| format!("{:?}", cause.kind()))
        .expect("TLS executor");
        let owner = Owner::<Wire, KeyTag<0>>::try_new(net.connections()).expect("TLS storage");
        exec.with_storage(owner).run(|exec| {
            let mut reports = Reports::<1>::new(drive, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
            let entered = exec.enter_with(
                |mut sess| {
                    let targets = Cell::new([None; 2]);
                    let armed = Cell::new(false);
                    let budget = sess
                        .driver()
                        .storage
                        .budget(driver_storage::Resident::bytes(2 * body.len()))
                        .expect("output resident domain");
                    let bodies = std::array::from_fn(|_| {
                        let charge = budget.try_charge(body.len()).expect("admitted output");
                        let mut bytes = storage::Owned::try_from_charge(charge)
                            .expect("resident output storage");
                        bytes.try_extend(body).expect("initialized reply");
                        Some(bytes.freeze())
                    });
                    let capacity = |event: Change<'_>| match event {
                        Change::Ready(pending) => {
                            assert!(matches!(pending, egress::Pending::Absent));
                        }
                        Change::Activated => {
                            started.hit();
                            if !forced {
                                assert_eq!(
                                    released.get(),
                                    [0; 2],
                                    "both owners precede CQ settlement"
                                );
                            }
                            assert_eq!(writes.get(), started.hits());
                        }
                        Change::Released {
                            index,
                            body: returned,
                            resident,
                            cause,
                        } => {
                            if matches!(cause, handler::Disposition::Closed) {
                                assert!(forced, "only forced output is discarded");
                                discarded.set(discarded.get() + 1);
                            } else {
                                assert!(matches!(cause, handler::Disposition::Sent));
                            }
                            assert_eq!(returned, body);
                            let mut counts = released.get();
                            assert_eq!(counts[index], 0, "one release per output owner");
                            assert_eq!(resident, body.len());
                            assert!(
                                budget.resident() >= resident,
                                "the callback still owns the charged body"
                            );
                            counts[index] += 1;
                            released.set(counts);
                        }
                    };
                    let store = sess.storage();
                    let wire =
                        endpoint::Config::from(Arc::new(key.server().expect("server policy")))
                            .with_send(NonZero::<usize>::MIN);
                    let Ok(endpoint) = store.bind(wire);
                    let init = listener::Install::<
                        KeyTag<0>,
                        Bundle<Tcp, Wire, timing::Throughput>,
                        _,
                    >::new(net, endpoint);
                    let hash = sess.hash_state(listener::Domain::for_listener(0));
                    let result = init.with_driver(
                        Capacity {
                            reply: Reply {
                                body: Vec::new(),
                                closes: closes.clone(),
                                observe: &observe,
                                mode: Mode::Close,
                            },
                            bodies,
                            targets: &targets,
                            armed: &armed,
                            ready: ready.clone(),
                            observe: &capacity,
                        },
                        hash,
                        &mut sess,
                        session::Scope::driver,
                        |listen, _, sess| {
                            peer::Peer::at(listen.local_addr())
                                .with_many::<2, _, _>(
                                    move |sockets| run_pair(sockets, pubkey),
                                    |client| {
                                        let mut finish = Completion::new();
                                        let driven = sess.with_quiesce(
                                            &mut finish,
                                            Host::new(listen),
                                            if forced { Duration::ZERO } else { TIMEOUT },
                                            |mut app| {
                                                let harness = fibers::Harness::new(GUARD);
                                                assert_eq!(
                                                    harness
                                                        .try_wait(&mut app, &ready, 2)
                                                        .expect("establish both TLS peers"),
                                                    fibers::Wait::Reached
                                                );
                                                assert_eq!(writes.get(), 0);
                                                armed.set(true);
                                                for target in targets.get() {
                                                    target.expect("established peer target").wake();
                                                }
                                                if forced {
                                                    assert_eq!(
                                                        harness
                                                            .try_wait(&mut app, &started, 2)
                                                            .expect("admit both queued replies"),
                                                        fibers::Wait::Reached
                                                    );
                                                    assert_eq!(
                                                        budget.resident(),
                                                        body.len(),
                                                        "one owner waits for ciphertext"
                                                    );
                                                    assert_eq!(discarded.get(), 0);
                                                } else {
                                                    assert_eq!(
                                                        harness
                                                            .try_wait(&mut app, &closes, 2)
                                                            .expect("drain both direct replies"),
                                                        fibers::Wait::Reached
                                                    );
                                                }
                                            },
                                        );
                                        assert_eq!(
                                            budget.resident(),
                                            0,
                                            "native quiescence releases the last output owner"
                                        );
                                        if forced {
                                            assert!(
                                                matches!(
                                                    &driven,
                                                    Err(session::Error::Quiesce {
                                                        output: (),
                                                        drive: drive::Error::QuiescenceTimeout,
                                                        disarm: None,
                                                    })
                                                ),
                                                "forced listener cleanup: {driven:?}"
                                            );
                                            assert_eq!(
                                                discarded.get(),
                                                1,
                                                "forced cleanup reports the waiting owner once"
                                            );
                                            drop(client.finish().expect("join quiesced TLS peers"));
                                            return;
                                        }
                                        assert!(driven.is_ok(), "listener shutdown");
                                        let replies = client
                                            .finish()
                                            .expect("finish native peers")
                                            .expect("TLS replies");
                                        let mut expected = PREFIX.to_vec();
                                        expected.extend_from_slice(body);
                                        for (closed, got, _) in replies {
                                            assert!(closed, "close_notify precedes TCP EOF");
                                            assert_eq!(got, expected);
                                        }
                                    },
                                )
                                .expect("spawn TLS peers");
                        },
                    );
                    assert!(result.is_ok(), "listener installation");
                },
                recovery::Policy::new(|report| reports.push(report), common::recover),
            );
            let events = reports.events().expect("reclamation observations");
            if !forced {
                assert!(
                    events.is_empty(),
                    "unexpected reclaimed completions: {reports:?}"
                );
            }
            assert!(entered.is_ok(), "listener scope");
        });
        assert_eq!(inputs.get(), 2, "peers send only their readiness records");
        assert_eq!(started.hits(), 2);
        if forced {
            // TLS drops the first queued body on submit without a release callback.
            assert_eq!(released.get().iter().sum::<usize>(), 1);
        } else {
            assert_eq!(released.get(), [1; 2]);
            assert_eq!(closes.hits(), 2);
        }
    }
}
