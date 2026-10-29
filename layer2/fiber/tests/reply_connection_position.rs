pub mod common;

use std::{cell::Cell, convert::Infallible, io::Read as _, num::NonZero};

use common::{
    runtime::Setup,
    storage::replies::{position, support},
};
use dope::{
    core::{
        driver::{
            life::recovery,
            route::{KeyTag, table::conn::Capacity},
        },
        io::socket,
    },
    manifold::{
        app::{Host, env::Bundle},
        client::{
            attempt::{StreamTarget, queue},
            conn::{self, engine},
        },
        timing::Balanced,
    },
    net::{
        tcp::Tcp,
        wire::{Identity, domain},
    },
    runtime::executor::{session, session::Completion},
};
use dope_fiber::reply::{
    self, layout,
    recv::{self, attach::Attach as _},
};
use dope_test::{
    fibers,
    harness::{observe::Trace, peer::TcpScript, scenario, signals::Gate},
};
use o3::{base::cell::region, depot::capacity};

#[test]
fn connection_selects_lane() {
    TcpScript::with_many::<2, _>(
        |streams| {
            for stream in streams {
                let mut byte = [0];
                assert_eq!(stream.read(&mut byte).expect("connection EOF"), 0);
            }
        },
        |server| {
            let connected = Gate::new();
            let checked = Cell::new(false);
            let errors = Trace::new();
            let mut finish = Completion::new();
            let mut reports = Vec::new();
            Setup::for_profile::<Balanced>().with_session(
                |mut session| {
                    let source = queue::Source::<Tcp, KeyTag<0>>::try_new(
                        capacity::Count::new(2),
                        &mut session.driver().storage,
                    )
                    .expect("attempt source");
                    let attempts = std::array::from_fn::<_, 2, _>(|_| {
                        source
                            .try_reserve()
                            .expect("attempt")
                            .start(StreamTarget::new(server.addr(), socket::option::Set::EMPTY))
                    });
                    engine::Startup::new(
                        position::Session {
                            check: check_reply_position,
                            connected: &connected,
                            checked: &checked,
                            errors: &errors,
                        },
                        &source,
                        Capacity::fixed::<2>(),
                        Default::default(),
                        domain::Local::new(Identity),
                    )
                    .try_with::<Bundle<Tcp, Identity, Balanced>, _, _>(
                        &mut session,
                        session::Scope::driver,
                        |connector, session| {
                            session.with_quiesce(
                                &mut finish,
                                Host::new(connector),
                                scenario::TIMEOUT,
                                |mut app| {
                                    assert_eq!(
                                        fibers::TEST
                                            .try_wait(&mut app, &connected, 2)
                                            .expect("connection progress"),
                                        fibers::Wait::Reached,
                                    );
                                    assert!(checked.get(), "second connection checked");
                                },
                            )
                        },
                    )
                    .expect("client machine")
                    .expect("client teardown");
                    for attempt in attempts {
                        attempt.cancel();
                    }
                },
                recovery::Policy::new(
                    |event| reports.push(event),
                    |failure: recovery::Recovery<'_, '_>| -> Infallible {
                        panic!("native recovery: {:?}", failure.cause());
                    },
                ),
                |result| result.expect("driver lifecycle"),
            );
            assert!(errors.values().expect("client observations").is_empty());
            assert!(
                reports.is_empty(),
                "unexpected reclaimed operations: {reports:?}"
            );
            server.finish().expect("connected peers");
        },
    )
    .expect("peer worker");
}

fn check_reply_position<'d>(
    conn: conn::Id<'d, KeyTag<0>>,
    token: &mut region::Token<region::Identity<'d>>,
) {
    assert_eq!(conn.index().into_usize(), 1);
    for lanes in [1, 2] {
        let layout = support::layout(lanes, lanes).expect("reply layout");
        reply::Arena::<()>::try_with(
            layout,
            token.proof(),
            |arena| match layout::Position::try_new(arena.domain(), conn) {
                Err(error) => {
                    assert_eq!(lanes, 1);
                    assert_eq!(
                        error,
                        reply::Error::Lane {
                            index: reply::LaneIndex::new::<1>(),
                            capacity: NonZero::<u32>::MIN,
                        }
                    );
                }
                Ok(position) => {
                    assert_eq!(lanes, 2);
                    assert!(position.connection() == conn);
                    arena.lane(position).control().activate(token);
                    let mut response = recv::Response::<_, support::Never>::new();
                    response
                        .try_attach(token, arena.lane(position))
                        .expect("reply attachment");
                    assert!(
                        arena.lane(position).control().attachment(token)
                            == reply::Attachment::Attached
                    );
                    assert!(
                        arena
                            .try_lane(reply::LaneIndex::ZERO)
                            .expect("first lane")
                            .control()
                            .attachment(token)
                            == reply::Attachment::Detached
                    );
                }
            },
        )
        .expect("reply arena");
    }
}
