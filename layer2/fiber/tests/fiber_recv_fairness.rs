use dope::{
    core::driver::{life::recovery, settings::Receive},
    manifold::network::listener::Domain,
    runtime::executor::{Executor, session::Completion},
};
use dope_core::{driver::route::KeyTag, platform::wake};
use dope_test::harness::{GUARD, scenario};
pub mod common;

use std::{
    convert::Infallible,
    io::{Read, Write},
    rc::Rc,
};

use dope::{
    core::{
        driver::{
            route::{FrameworkTag, SLOT_MASK, Tag, kind, table::conn::Capacity},
            settings::{self, claim::Needs},
            storage::Value,
        },
        io::socket::Backlog,
    },
    manifold::timing::Balanced,
    net::{
        tcp::Tcp,
        wire::{Identity, terminal},
    },
    runtime::executor::drive,
};
use dope_fiber::{
    BlockOn as _,
    net::{
        client,
        server::{Config, Listener, Plan},
    },
};
use dope_test::{
    fibers,
    harness::{peer::Peer, signals::Gate},
    reclaim::Reports,
};
use o3::buffer::write::consume::Consume as _;

use crate::common::network::{fair::*, reserve::Reservation};

#[test]
fn capacity_before_build() {
    let max = Capacity::try_from(SLOT_MASK as usize).expect("maximum route capacity");
    let listen = Plan::<Identity>::new(max, Identity);
    let dial = client::Plan::<Tcp, Identity>::new(max, Identity);
    assert!(Needs::from_tree(&listen).is_ok());
    assert!(Needs::from_tree(&dial).is_ok());
}

#[test]
fn hoarder_yields_to_peer() {
    let mut finish = Completion::new();
    let bound = (Reservation::bind()).expect("reserve listener");
    let addr = bound.addr();
    let seen = Gate::new();
    let owner = Endpoint::try_new(seen.clone()).expect("fairness receive slots");
    let limits = Rc::clone(&owner.limits);
    let plan = Plan::<PooledWire>::new(Capacity::fixed::<MAX_CONN>(), owner);
    let config = Needs::from_tree(&plan)
        .and_then(|needs| needs.resolve::<Balanced>())
        .expect("listener driver configuration must succeed");
    assert!((config.file_slots().accept().count()) == (Capacity::fixed::<MAX_CONN>().entries()));
    assert!(
        (config.file_slots().outbound()) == (<Balanced as settings::Profile>::FILES.outbound())
    );
    use dope::core::driver::life::creation::Attempt;
    let mut create = Attempt::new();
    let exec = (Executor::new(
        &mut create,
        config.with_receive(Receive::fixed::<1024, 1>()),
    ))
    .ok()
    .expect("executor must initialize");
    let mut reports = Reports::<_>::new(config, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    exec
            .with_plan(Value::new(plan)).run(|executor| {
    let result = executor.enter_with(move |mut sess| {
                let hash = sess.hash_state(Domain::DEFAULT);
                let plan = sess.storage();
                let store =
                    (plan.try_build(sess.driver().storage)).expect("listener port must initialize");
                let end = match plan.wire_domain().bind(()) {
                    Ok(end) => end,
                    Err(never) => match never {},
                };
                let result = Listener::<KeyTag<0>, Tcp, PooledWire, _>::adopt(
                    &store,
                    Config {
                        direct_flights: 0.into(),
                        bind: addr,
                        backlog: Backlog::fixed::<16>(),
                        stream: Default::default(),
                        transport: Default::default(),
                        egress: Default::default(),
                        bound: bound.into_tcp(),
                        wire: end,
                    },
                    |cause| cause.eliminate(),
                    hash,
                    &mut sess,
                    |sess| sess.driver(),
                    |server, sess| {
                        let pool = store.handle();
                        let (rx, tx) = wake::Ends::blocking().expect("probe release").split();
                        Peer::at(addr)
                            .with_many::<2, _, _>(move |[bulk, probe]| {
                                bulk.write_all(&[b'H'; QUEUE_CAP])
                                    .expect("fill receive queue");
                                bulk.flush().expect("flush receive queue");
                                rx.wait_timeout(GUARD)
                                    .expect("hoarder must retain all input");
                                probe.write_all(b"P").expect("write probe");
                                let mut reply = [0; 1];
                                probe.read_exact(&mut reply).expect("read probe reply");
                                reply
                            }, |peers| {
                        let result = sess.with_quiesce(&mut finish,
                            App { pool: server }, scenario::TIMEOUT,
                            |mut app| -> Result<(), drive::Error> {
                                let bulk = app.block_on(
                                    dope_fiber::fiber!('_ => async move { pool.accept().await }),
                                )?;
                                assert!(bulk.is_ok(), "accept hoarder");
                                let probe = (app.block_on(
                                    dope_fiber::fiber!('_ => async move { pool.accept().await }),
                                )?).ok().expect("accept probe");
                                assert_eq!(fibers::TEST.try_wait(&mut app, &seen, QUEUE_CAP as u32).expect("bounded progress"), fibers::Wait::Reached);
                                assert!(tx.notify().is_ok(), "probe peer must remain connected");
                                let mut probe = Some(probe);
                                app.block_on(dope_fiber::fiber!('_ => async move {
                                    let mut probe = probe.take().expect("probe owner");
                                    {
                                        let read = probe.read().await;
                                        assert!(read.is_ok(), "read typed probe input: {:?}",
                                            read.as_ref().err().map(|error| error.map_ref(terminal::Error::causes)));
                                        let mut request = read.ok().flatten().expect("probe input before EOF");
                                        assert_eq!(request.as_ref(), b"P");
                                        assert_eq!(request.consume_max(1), 1);
                                    }
                                    let reply = probe.write_all(b"R").await;
                                    assert!(reply.is_ok(), "write typed probe reply");
                                }))?;
                                Ok(())
                            },
                        );
                        let result = result.expect("application teardown must succeed");
                        {
                            ((result).expect("fairness fiber stalled"));
                        }
                        assert_eq!(peers.finish().expect("finish fairness clients"), [b'R']);
                        }).expect("spawn fairness clients");
                    },
                );
                assert!(result.is_ok(), "listener must bind");
            }, recovery::Policy::new(|event| reports.push(event), recover));
        assert!(
            reports.events().expect("reclamation observations").is_empty(),
            "unexpected reclaimed operations: {reports:?}"
        );

        assert!(result.is_ok(), "listener port storage must initialize");
        });
    let limits = limits.get().expect("wire bootstrap limits observed");
    assert!(limits.retained().into_raw() >= QUEUE_CAP);
    assert_eq!(limits.receive_len().into_nonzero().get(), 1);
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
