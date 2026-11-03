pub mod common;

use std::{cell::Cell, convert::Infallible, net, pin, time::Duration};

use common::{
    peer, recovery as teardown,
    retry::{
        BIND, ENDPOINT, Events, socket,
        traces::Immediate,
        writable::{Callback, Connected},
    },
    suite::peers::tls,
};
use dope::{
    core::{
        Errno,
        driver::{
            life::{creation::Attempt, recovery},
            result::SubmitError,
            route::{Id, KeyTag},
        },
        io::datagram::ReceiveError,
    },
    manifold::{app::dispatch::raw::Projection as _, network::datagram::hook::ingress},
    runtime::{
        executor::{Executor, session},
        shutdown,
    },
};
use dope_quic::{
    client::{self, build, protocol},
    conn::life::options::Options,
    runtime::endpoint::client::Plan,
};
use dope_test::harness::scenario::TIMEOUT;
use o3::depot::arena::slab::Vacancy;

#[test]
fn retains_terminal_socket_failure() {
    let observed = Cell::new(None);
    let (_, public) = peer::raw(0x2c).expect("raw identity");
    let plan = Plan::<1>::new(BIND, ENDPOINT).expect("client endpoint");
    let mut create = Attempt::new();
    let executor = Executor::new(
        &mut create,
        Events::settings(8, plan.as_ref()).expect("driver claims"),
    );
    executor
        .as_ref()
        .map_err(|cause| cause.kind())
        .expect("driver");
    executor.ok().expect("driver admission").run(|executor| {
        executor
            .enter_with(
                |mut session| {
                    let mut driver = session.driver();
                    let client = build::Builder::owned(
                        vec![client::EndpointSpec {
                            addr: BIND,
                            authority: tls::client(public).expect("TLS authority"),
                        }],
                        Options::default(),
                        Connected::new(|event: Result<client::SlotId, Callback<'_>>| {
                            let Err(Callback::SocketReceive(error)) = event else {
                                panic!("unexpected callback: {event:?}");
                            };
                            assert!(observed.replace(Some(*error)).is_none());
                        })
                        .with_socket_events(),
                        Immediate::default(),
                        client::Config {
                            event_budget: 1,
                            retry_budget: 1,
                        },
                    )
                    .expect("client topology")
                    .bind(plan, &mut driver)
                    .expect("bound client");
                    let mut client = pin::pin!(client);
                    let clock = driver.driver_ref().clock();
                    let fatal = SubmitError::from(Vacancy::IdentityExhausted);
                    let full = ReceiveError::Submit(SubmitError::from(Vacancy::Full));
                    for (error, expected) in [
                        (ReceiveError::Socket(Errno::INVALID_ARGUMENT), None),
                        (full, None),
                        (
                            ReceiveError::Submit(fatal),
                            Some(protocol::Failure::Submit(fatal)),
                        ),
                        (full, Some(protocol::Failure::Submit(fatal))),
                    ] {
                        <ingress::Coupled as ingress::Failure<'_, KeyTag<1>, _>>::failed(
                            client.as_mut().inner_mut().inner_mut(),
                            error,
                            clock.now(),
                            driver.state_token(),
                        );
                        assert_eq!(client.failure(), expected);
                        assert_eq!(observed.take(), Some(error));
                    }
                },
                recovery::Policy::new(
                    |report| panic!("unexpected native retirement: {report:?}"),
                    |failure: recovery::Recovery<'_, '_>| -> Infallible {
                        panic!("native recovery: {:?}", failure.cause())
                    },
                ),
            )
            .as_ref()
            .map_err(|cause| cause.cause())
            .expect("driver retirement");
    });
}

#[test]
fn reports_native_send_failure() {
    let (_, public) = peer::raw(0x2d).expect("raw identity");
    let destination = net::SocketAddr::from((net::Ipv6Addr::LOCALHOST, 9));
    let native = net::UdpSocket::bind(BIND)
        .expect("IPv4 error oracle")
        .send_to(b"family", destination)
        .expect_err("IPv4 socket cannot send to IPv6");
    let expected = Errno::try_from(native.raw_os_error().expect("native error code"))
        .expect("native address-family error");
    let settled = Cell::new(0);
    let checked = Cell::new(false);
    let plan = Plan::<1>::new(BIND, ENDPOINT).expect("IPv4 client endpoint");
    let (source, stop) = shutdown::Pair::new().expect("shutdown pair").split();
    let mut create = Attempt::new();
    let executor = Executor::new(
        &mut create,
        Events::settings(8, plan.as_ref()).expect("driver claims"),
    );
    executor
        .as_ref()
        .map_err(|cause| cause.kind())
        .expect("driver");
    let executor = executor
        .ok()
        .expect("driver admission")
        .shutdown(source)
        .expect("shutdown source");
    let mut reports = Vec::new();
    executor.run(|executor| {
        executor
            .enter_with(
                |mut session| {
                    let (client, timeout) = {
                        let mut driver = session.driver();
                        let timeout = driver
                            .driver_ref()
                            .clock()
                            .now()
                            .try_add(Duration::from_secs(2))
                            .expect("test deadline");
                        let client = build::Builder::owned(
                            vec![client::EndpointSpec {
                                addr: destination,
                                authority: tls::client(public).expect("TLS authority"),
                            }],
                            Options::default(),
                            Connected::new(
                                |event: Result<client::SlotId, Callback<'_>>| match event {
                                    Err(Callback::SocketSend {
                                        destination: actual,
                                        payload,
                                        error,
                                    }) => {
                                        assert_eq!(actual, destination);
                                        assert_eq!(error, expected);
                                        assert!(!payload.is_empty());
                                        settled.set(settled.get() + 1);
                                    }
                                    Err(Callback::Close) => {}
                                    event => panic!("unexpected callback: {event:?}"),
                                },
                            )
                            .with_socket_events(),
                            Immediate::default(),
                            client::Config {
                                event_budget: 1,
                                retry_budget: 1,
                            },
                        )
                        .expect("client topology")
                        .bind(plan, &mut driver)
                        .expect("bound client");
                        (client, timeout)
                    };
                    let mut report = session::Completion::new();
                    session
                        .with_quiesce(
                            &mut report,
                            socket::App {
                                client,
                                settled: &settled,
                                checked: &checked,
                                stop: Some(stop),
                                timeout,
                            },
                            TIMEOUT,
                            |app| app.run_for(TIMEOUT).map(|_| ()),
                        )
                        .expect("session completion")
                        .expect("application completion");
                },
                recovery::Policy::new(
                    |event| reports.push(event),
                    |failure: recovery::Recovery<'_, '_>| -> Infallible {
                        panic!("native recovery: {:?}", failure.cause())
                    },
                ),
            )
            .as_ref()
            .map_err(|cause| cause.cause())
            .expect("driver retirement");
    });
    assert!(checked.get(), "native failure leaves the endpoint usable");
    teardown::Teardown::new([Id::fixed::<1>()])
        .verify(reports)
        .expect("exact receive retirement");
}
