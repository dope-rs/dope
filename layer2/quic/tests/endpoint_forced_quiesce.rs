pub mod common;

use std::{convert::Infallible, time::Duration};

use common::{
    peer,
    retry::{BIND, ENDPOINT, Events, traces::Immediate},
    runtime::forced::{App, Client, Observed, PAYLOAD, Server},
    suite::peers::tls,
};
use dope::{
    core::driver::life::{creation::Attempt, recovery},
    runtime::{
        executor::{Executor, drive, session},
        shutdown,
    },
};
use dope_quic::{
    client::{self, EndpointSpec, build},
    conn::life::options::Options,
    proto::param,
    runtime::{endpoint, receive},
    stream::release::Cause,
};
use dope_test::harness::scenario::TIMEOUT;

#[test]
fn forced_finish_returns_owners_and_delivers_close() {
    let (signing, public) = peer::raw(0x37).expect("identity");
    let params = param::Set::builder()
        .idle_ms(30_000)
        .initial_data(0)
        .local_bidi(0)
        .remote_bidi(0)
        .bidi_count(1)
        .try_build()
        .expect("zero stream credit");
    let server = endpoint::server::Plan::<0, receive::Copied>::from_params(
        BIND,
        signing.server().expect("server TLS"),
        params.clone(),
        ENDPOINT,
    )
    .expect("server plan");
    let client = endpoint::client::Plan::<1>::new(BIND, ENDPOINT).expect("client plan");
    let (source, stop) = shutdown::Pair::new().expect("shutdown pair").split();
    let observed = Observed::default();
    observed.stop.set(Some(stop));
    let mut creation = Attempt::new();
    let executor = Executor::new(
        &mut creation,
        Events::settings(64, (server.as_ref(), client.as_ref())).expect("driver claims"),
    )
    .ok()
    .expect("executor")
    .shutdown(source)
    .expect("shutdown source");
    executor.run(|executor| {
        executor
            .enter_with(
                |mut session| {
                    let (server, client) = {
                        let mut driver = session.driver();
                        let server = server.bind(Server(&observed), &mut driver).expect("server");
                        let client = build::Builder::owned(
                            vec![EndpointSpec {
                                addr: server.local_addr(),
                                authority: tls::client(public).expect("client authority"),
                            }],
                            Options::try_from(params).expect("client options"),
                            Client(&observed),
                            Immediate::default(),
                            client::Config {
                                event_budget: 1,
                                retry_budget: 1,
                            },
                        )
                        .bind(client, &mut driver)
                        .expect("client");
                        (server, client)
                    };
                    let mut completion = session::Completion::new();
                    let result = session.with_quiesce(
                        &mut completion,
                        App { server, client },
                        Duration::ZERO,
                        |app| app.run_for(TIMEOUT).map(|_| ()),
                    );
                    assert!(matches!(
                        result,
                        Err(session::Error::Quiesce {
                            output: Ok(()),
                            drive: drive::Error::QuiescenceTimeout,
                            disarm: None,
                        })
                    ));
                },
                recovery::Policy::new(
                    |_| {},
                    |failure: recovery::Recovery<'_, '_>| -> Infallible {
                        panic!("native recovery: {:?}", failure.cause())
                    },
                ),
            )
            .as_ref()
            .map_err(|error| error.cause())
            .expect("driver retirement");
    });
    assert_eq!(observed.ready.get(), 2);
    assert!(observed.server_closed.get());
    assert!(observed.client_closed.get());
    let release = observed.released.get().expect("one retained payload");
    assert!(matches!(release.cause, Cause::Closed));
    assert_eq!(release.charge.logical_bytes, PAYLOAD.len());
    assert_eq!(release.charge.owners, 1);
}
