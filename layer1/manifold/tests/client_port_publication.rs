pub mod common;

use std::{cell::Cell, convert::Infallible, mem, num::NonZero, rc::Rc, time::Duration};

use dope_core::driver::{
    life::{creation::Attempt, recovery},
    route::{Id, KeyTag, table::conn::Capacity},
    settings,
};
use dope_manifold::{
    app::{Host, env::Bundle},
    client::{
        attempt,
        close::{Directive, Reason},
        conn::engine,
        deadline,
        port::{Port, write::Reserve},
    },
    timing::Throughput,
};
use dope_net::{
    Transport as _,
    link::egress::{self, metadata::arena::Store},
    tcp::{self, Tcp},
    wire::domain::Local,
};
use dope_runtime::executor::{
    Executor,
    session::{Completion, Scope},
};
use dope_test::{
    fibers,
    harness::{GUARD, peer::TcpAcceptor, signals::Gate},
    reclaim::Reports,
};
use o3::{buffer::pool, depot::capacity};

use crate::common::{
    contract::Report,
    manifold::client::{
        connect::ports::{App, NESTED, PAYLOAD, cursor},
        send::wire::Wire,
    },
    scenario::case::attempt::Attempts,
};

#[test]
fn publication_keeps_owner() {
    let peer = TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("bind peer");
    let config = settings::Config::for_profile::<Throughput>();
    let mut create = Attempt::new();
    let mut finish = Completion::new();
    let mut reports = Reports::<_>::new(config, [Id::fixed::<0>()]);
    let audit = Rc::new(Report::default());
    let result = Executor::new(&mut create, config);
    result
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor construction");
    let executor = result.ok().expect("executor construction");
    executor
        .with_plan(Attempts::<KeyTag<0>>::new(capacity::Count::new(1)))
        .run(|executor| {
            let result = executor.try_create(
                |mut session| {
                    let source = session.storage();
                    let attempt = source.try_reserve().expect("attempt slot").start(
                        attempt::StreamTarget::new(
                            peer.local_addr().expect("peer address"),
                            Tcp::options(tcp::StreamConfig::default()),
                        ),
                    );
                    let mut stores = (0..6)
                        .map(|_| {
                            Store::try_new(
                                egress::Config::shared(egress::Credit::fixed::<2, 32>()),
                                NonZero::<usize>::MIN,
                            )
                            .expect("port store")
                        })
                        .collect::<Vec<_>>();
                    let ports = stores
                        .iter_mut()
                        .map(|store| {
                            Port::try_new(store, &session.driver().regions().0).expect("port")
                        })
                        .collect();
                    let done = Gate::new();
                    let app = App::new(
                        ports,
                        done.clone(),
                        Rc::clone(&audit),
                        |ports, conn, work, _, token| {
                            let buffers = pool::Pool::try_new(2, 8).expect("payload pool");
                            for (case, port) in ports.iter().enumerate() {
                                assert!(matches!(
                                    port.pending(conn),
                                    Some(egress::Pending::Absent)
                                ));
                                if case == 4 {
                                    assert!(port.finish_write(conn).is_ok(), "port shutdown");
                                    assert!(matches!(
                                        port.pending(conn),
                                        Some(egress::Pending::Present)
                                    ));
                                    let mut cancels = 0;
                                    assert!(matches!(
                                        port.retire(conn, work, token, |_| cancels += 1),
                                        egress::Cleared::Done
                                    ));
                                    assert_eq!(cancels, 0);
                                    assert!(port.pending(conn).is_none());
                                    continue;
                                }
                                let payload = cursor(&buffers, PAYLOAD)
                                    .expect("payload fits")
                                    .expect("payload capacity");
                                let address = payload.as_ref().as_ptr();
                                'publication: {
                                    let reserve = port.try_reserve(token, conn, payload);
                                    assert!(matches!(reserve, Reserve::Item(_)));
                                    let Reserve::Item(hold) = reserve else {
                                        return None;
                                    };
                                    if case == 5 {
                                        assert!(port.finish_write(conn).is_ok());
                                        let called = Cell::new(false);
                                        let result = hold
                                            .validate()
                                            .map(|permit| permit.commit(|_| called.set(true)));
                                        assert!(!called.get());
                                        let payload = result.expect_err("write sealed");
                                        assert_eq!(payload.into_inner().as_ref().as_ptr(), address);
                                        break 'publication;
                                    }
                                    let permit =
                                        hold.validate().ok().expect("validated generation");
                                    if case == 3 {
                                        mem::forget(permit);
                                    } else {
                                        let early = Cell::new(0);
                                        let during = |token: &mut _| {
                                            assert!(matches!(
                                                port.retire(conn, work, token, |_| {
                                                    early.set(early.get() + 1)
                                                }),
                                                egress::Cleared::Retry
                                            ));
                                            assert_eq!(early.get(), 0);
                                            let nested = cursor(&buffers, NESTED)
                                                .expect("nested fits")
                                                .expect("nested capacity");
                                            assert!(matches!(
                                                port.try_reserve(token, conn, nested),
                                                Reserve::Inactive(_)
                                            ));
                                        };
                                        if case == 0 {
                                            permit.commit(during);
                                        } else {
                                            let result = if case == 1 {
                                                permit.try_commit(|token| {
                                                    during(token);
                                                    Err::<(), _>(case)
                                                })
                                            } else {
                                                permit.try_with(|token, _| {
                                                    during(token);
                                                    Err::<(), _>(case)
                                                })
                                            };
                                            let failure = result.expect_err("transition rejection");
                                            let (payload, cause) = failure.into_parts();
                                            assert_eq!(cause, case);
                                            assert_eq!(
                                                payload.into_inner().as_ref().as_ptr(),
                                                address
                                            );
                                        }
                                    }
                                }
                                let mut cancels = 0;
                                assert!(matches!(
                                    port.retire(conn, work, token, |payload| {
                                        cancels += 1;
                                        assert_eq!(payload.as_ref().as_ptr(), address);
                                    }),
                                    egress::Cleared::Done
                                ));
                                assert_eq!(cancels, usize::from(case == 0));
                                assert!(!port.is_active(conn));
                                assert!(matches!(
                                    port.retire(conn, work, token, |_| cancels += 1),
                                    egress::Cleared::Done
                                ));
                                assert_eq!(cancels, usize::from(case == 0));
                            }
                            None
                        },
                    );
                    let result = engine::Startup::new(
                        app,
                        source,
                        Capacity::fixed::<1>(),
                        egress::Config::default(),
                        Local::new(()),
                    )
                    .try_with::<Bundle<Tcp, Wire, Throughput>, _, _>(
                        &mut session,
                        Scope::driver,
                        |engine, session| {
                            session.with_quiesce(
                                &mut finish,
                                Host::new(engine),
                                Duration::from_secs(2),
                                |mut running| {
                                    let wait = fibers::TEST.try_wait(&mut running, &done, 1);
                                    let faults = audit.faults.values().expect("port observations");
                                    assert!(faults.is_empty(), "{faults:?}");
                                    assert_eq!(
                                        wait.expect("publication progress"),
                                        fibers::Wait::Reached
                                    );
                                },
                            )
                        },
                    );
                    assert!(matches!(result, Ok(Ok(()))), "engine lifecycle");
                    attempt.cancel();
                },
                recovery::Policy::new(
                    |event| reports.push(event),
                    |failure: recovery::Recovery<'_, '_>| -> Infallible {
                        panic!("native recovery: {:?}", failure.cause())
                    },
                ),
            );
            assert!(result.is_ok(), "executor teardown");
        });
    assert!(
        matches!(reports.events(), Ok(events) if events.is_empty()),
        "unexpected reclaim: {reports:?}"
    );
    let faults = audit.faults.values().expect("port observations");
    assert!(faults.is_empty(), "{faults:?}");
    assert_eq!(
        (audit.sent.get(), audit.cancelled.get()),
        (Some(0), Some(0))
    );
    let stream = peer
        .accept_for(GUARD)
        .expect("accept peer")
        .expect("connected peer");
    stream.read_wait(Some(GUARD)).expect("peer read deadline");
    assert_eq!(stream.read(&mut [0]).expect("peer teardown"), 0);
}

#[test]
fn deadline_cuts_retiring_queue() {
    for (queued_close, retired, incoming) in [
        (false, false, false),
        (true, false, false),
        (false, false, true),
        (false, true, false),
    ] {
        let peer = TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("peer");
        let config = settings::Config::for_profile::<Throughput>();
        let mut create = Attempt::new();
        let mut finish = Completion::new();
        let audit = Rc::new(Report::default());
        let executor = Executor::new(&mut create, config).ok().expect("executor");
        executor
            .with_plan(Attempts::<KeyTag<0>>::new(capacity::Count::new(1)))
            .run(|executor| {
                let result = executor.try_create(
                    |mut session| {
                        let source = session.storage();
                        let attempt = source.try_reserve().expect("attempt").start(
                            attempt::StreamTarget::new(
                                peer.local_addr().expect("address"),
                                Tcp::options(tcp::StreamConfig::default()),
                            ),
                        );
                        let mut store = Store::try_new(
                            egress::Config::shared(egress::Credit::fixed::<1, 32>()),
                            NonZero::<usize>::MIN,
                        )
                        .expect("port storage");
                        let port =
                            Port::try_new(&mut store, &session.driver().regions().0).expect("port");
                        let app = App::new(
                            vec![port],
                            Gate::new(),
                            Rc::clone(&audit),
                            |ports, conn, work, queue, token| {
                                let buffers = pool::Pool::try_new(1, 8).expect("payload pool");
                                let payload = cursor(&buffers, PAYLOAD)
                                    .expect("payload size")
                                    .expect("payload");
                                assert!(matches!(
                                    queue.try_enqueue(token, payload),
                                    egress::Enqueue::Committed
                                ));
                                let retire = Directive::reconnect(Reason::EndpointRetired);
                                if queued_close {
                                    assert!(ports[0].finish_write(conn).is_ok());
                                    ports[0].close(conn, retire);
                                }
                                if retired {
                                    assert!(matches!(
                                        ports[0].retire(conn, work, token, drop),
                                        egress::Cleared::Done
                                    ));
                                }
                                if !incoming {
                                    ports[0].close(
                                        conn,
                                        Directive::cut_retry(Reason::Timeout(
                                            deadline::Kind::Inbound,
                                        )),
                                    );
                                }
                                if queued_close {
                                    assert!(ports[0].finish_write(conn).is_ok());
                                    None
                                } else {
                                    Some(retire)
                                }
                            },
                        );
                        let done = app.done.clone();
                        let closed = app.closed.clone();
                        let reason = Rc::clone(&app.reason);
                        let result = engine::Startup::new(
                            app,
                            source,
                            Capacity::fixed::<1>(),
                            egress::Config::default(),
                            Local::new(()),
                        )
                        .try_with::<Bundle<Tcp, Wire, Throughput>, _, _>(
                            &mut session,
                            Scope::driver,
                            |engine, session| {
                                session.with_quiesce(
                                    &mut finish,
                                    Host::new(engine),
                                    Duration::from_secs(2),
                                    |mut running| {
                                        let stream = if incoming {
                                            assert_eq!(
                                                fibers::TEST
                                                    .try_wait(&mut running, &done, 1)
                                                    .expect("queued output"),
                                                fibers::Wait::Reached,
                                            );
                                            let stream = peer
                                                .accept_for(GUARD)
                                                .expect("accept")
                                                .expect("connected");
                                            stream.write(&[1]).expect("receive activation");
                                            Some(stream)
                                        } else {
                                            None
                                        };
                                        assert_eq!(
                                            fibers::TEST
                                                .try_wait(&mut running, &closed, 1)
                                                .expect("immediate retirement"),
                                            fibers::Wait::Reached,
                                        );
                                        assert_eq!(reason.get(), Some(Reason::EndpointRetired));
                                        let stream = stream.unwrap_or_else(|| {
                                            peer.accept_for(GUARD)
                                                .expect("accept")
                                                .expect("connected")
                                        });
                                        stream.read_wait(Some(GUARD)).expect("peer deadline");
                                        assert_eq!(stream.read(&mut [0]).expect("peer closed"), 0);
                                    },
                                )
                            },
                        );
                        assert!(matches!(result, Ok(Ok(()))), "engine lifecycle");
                        attempt.cancel();
                    },
                    recovery::Policy::new(
                        |_| panic!("unexpected native reclaim"),
                        crate::common::recover,
                    ),
                );
                assert!(result.is_ok(), "executor teardown");
            });
        assert!(audit.faults.values().expect("faults").is_empty());
        assert_eq!(
            (audit.sent.get(), audit.cancelled.get()),
            (Some(0), Some(1))
        );
    }
}
