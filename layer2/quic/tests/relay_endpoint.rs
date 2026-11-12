pub mod common;

use std::{convert::Infallible, num::NonZero, time::Instant};

use dope::core::driver::{
    life::{
        creation::Attempt,
        recovery::{Policy, Recovery},
    },
    settings::{self, claim::Needs},
};
use dope::core::io::datagram::payload;
use dope::manifold::app::Host;
use dope::runtime::executor::{Executor, session};
use dope::runtime::topology::Worker;
use dope_quic::{
    conn::life::{accept, options::Options, tls::servers},
    runtime::{
        endpoint::{self, config, owner::pool, server},
        mux::{app, builder},
        receive, routing,
    },
    security::relay::{Key, Ticket},
};
use dope_test::{
    fibers,
    harness::{peer::udp::Socket, scenario::TIMEOUT},
    scope::Driver,
};

use common::routing::{CONFIG, Idle, capture};

#[test]
fn receive_extent_keeps_inner_capacity() {
    let bind = routing::Single::new(([127, 0, 0, 1], 0).into());
    let direct = endpoint::Plan::<0, receive::Copied>::new(CONFIG).expect("direct plan");
    let relay = endpoint::Plan::<0, receive::Copied>::single(CONFIG, &bind).expect("relay plan");
    let baseline = config::Config::driver(settings::Datagram::fixed::<2, 1200>());
    let direct = Needs::from_tree(&direct)
        .expect("direct claims")
        .apply_exact(baseline)
        .expect("direct driver");
    let relay = Needs::from_tree(&relay)
        .expect("relay claims")
        .apply_exact(baseline)
        .expect("relay driver");
    assert_eq!(
        relay.receive().buffer_len().widen().get(),
        direct.receive().buffer_len().widen().get() + Ticket::LEN,
    );
}

#[test]
fn outer_packet_bound_is_checked_before_native_binding() {
    let bind = routing::Single::new(([127, 0, 0, 1], 0).into());
    let mut config = CONFIG;
    config.packet_buffer_bytes = 1199;
    assert!(matches!(
        endpoint::Plan::<0, receive::Copied>::single(config, &bind),
        Err(config::Error::PacketBytes { got: 1199 }),
    ));
    for (address, maximum) in [
        (([127, 0, 0, 1], 0).into(), 65_507_u32),
        ("[::1]:0".parse().expect("IPv6 address"), 65_527),
    ] {
        let bind = routing::Single::new(address);
        config.packet_buffer_bytes = maximum - Ticket::LEN as u32;
        assert!(endpoint::Plan::<0, receive::Copied>::single(config, &bind).is_ok());
        for inner in [config.packet_buffer_bytes + 1, u16::MAX as u32] {
            config.packet_buffer_bytes = inner;
            assert!(endpoint::Plan::<0, receive::Copied>::new(config).is_ok());
            assert!(matches!(
                endpoint::Plan::<0, receive::Copied>::single(config, &bind),
                Err(config::Error::Extent(payload::ExtentError::TooLarge {
                    actual, maximum: limit,
                })) if actual.get() == inner as usize + Ticket::LEN
                    && u32::from(limit.get()) == maximum,
            ));
        }
    }
}

#[test]
fn outgoing_budget_charges_every_prefix() {
    let bind = routing::Single::new(([127, 0, 0, 1], 0).into());
    let mut config = CONFIG;
    config.outgoing_bytes_capacity = 1 << 30;
    assert!(endpoint::Plan::<0, receive::Copied>::new(config).is_ok());
    assert!(matches!(
        endpoint::Plan::<0, receive::Copied>::single(config, &bind),
        Err(config::Error::Capacity(builder::Error::Bytes { got, max }))
            if max == 1 << 30 && got == max + config.outgoing_capacity * Ticket::LEN,
    ));
    config.outgoing_bytes_capacity = usize::MAX;
    assert!(matches!(
        endpoint::Plan::<0, receive::Copied>::single(config, &bind),
        Err(config::Error::OutgoingPrefix { packets, bytes, prefix })
            if packets == config.outgoing_capacity
                && bytes == usize::MAX
                && usize::from(prefix.get()) == Ticket::LEN,
    ));
}

#[test]
fn native_binding_rejects_mismatched_framing() {
    let (identity, _) = common::peer::raw(0x36).expect("identity");
    let source = identity.server().expect("server");
    let authority = accept::Authority::new(source.clone(), Options::default()).expect("authority");
    let pool = servers::Pool::<0>::new(authority, NonZero::<usize>::MIN);
    let address = ([127, 0, 0, 1], 0).into();
    let bind = routing::Single::new(address);
    let direct = endpoint::Plan::<0, receive::Copied>::new(CONFIG).expect("direct plan");
    let relay = endpoint::Plan::<0, receive::Copied>::single(CONFIG, &bind).expect("relay plan");
    let owned = server::Plan::<0, receive::Copied>::single(
        routing::Single::new(address),
        source,
        Options::default(),
        CONFIG,
    )
    .expect("owned relay plan");
    let settings = Needs::from_tree(owned.as_ref())
        .expect("claims")
        .apply_exact(config::Config::driver(
            settings::Datagram::fixed::<2, 1200>(),
        ))
        .expect("driver configuration");
    Driver::new(settings).with_driver(
        |mut driver| {
            assert!(matches!(
                pool::Binding::new(&pool).bind_single(bind, capture(), direct, &mut driver),
                Err(endpoint::BindError::Framing { plan: None, socket: Some((prefix, _)) })
                    if usize::from(prefix.extent().get()) == Ticket::LEN,
            ));
            assert!(matches!(
                pool::Binding::new(&pool).bind(address, capture(), relay, &mut driver),
                Err(endpoint::BindError::Framing { plan: Some((prefix, _)), socket: None })
                    if usize::from(prefix.extent().get()) == Ticket::LEN,
            ));
            let ipv6 = routing::Single::new("[::1]:0".parse().expect("IPv6 address"));
            let ipv6_plan =
                endpoint::Plan::<0, receive::Copied>::single(CONFIG, &ipv6).expect("IPv6 plan");
            assert!(matches!(
                pool::Binding::new(&pool).bind_single(
                    routing::Single::new(address), capture(), ipv6_plan, &mut driver,
                ),
                Err(endpoint::BindError::Framing {
                    plan: Some((_, expected)), socket: Some((_, actual)),
                }) if expected.get() == 65_527 && actual.get() == 65_507,
            ));
            let endpoint = owned
                .bind(capture(), &mut driver)
                .expect("owned unsharded native UDP binding");
            assert!(endpoint.local_addr().port() != 0);
            assert!(endpoint.is_empty());
        },
        Policy::new(
            |_| {},
            |failure: Recovery<'_, '_>| -> Infallible {
                panic!("native recovery: {:?}", failure.cause())
            },
        ),
        |result| {
            if let Err(error) = result {
                panic!("driver lifecycle: {:?}", error.cause());
            }
        },
    );
}

#[test]
#[ignore = "requires native reuse-port routing"]
fn routed_binding_rejects_mismatched_framing() {
    let (identity, _) = common::peer::raw(0x37).expect("identity");
    let authority = accept::Authority::new(identity.server().expect("server"), Options::default())
        .expect("authority");
    let pool = servers::Pool::<0>::new(authority, NonZero::<usize>::MIN);
    let address = ([127, 0, 0, 1], 0).into();
    let count = routing::Count::try_from(2).expect("two shards");
    let mut relayed_group =
        routing::Group::bind_relay(address, count).expect("relay routing group");
    let relayed = relayed_group.split().next().expect("relay shard binding");
    let mut direct_group = routing::Group::bind(address, count).expect("direct routing group");
    let direct = direct_group.split().next().expect("direct shard binding");
    let direct_plan = endpoint::Plan::<0, receive::Copied>::new(CONFIG).expect("direct plan");
    let relay_plan =
        endpoint::Plan::<0, receive::Copied>::routed(CONFIG, &relayed).expect("routed relay plan");
    let settings = Needs::from_tree(&relay_plan)
        .expect("claims")
        .apply_exact(config::Config::driver(
            settings::Datagram::fixed::<2, 1200>(),
        ))
        .expect("driver configuration");
    let run =
        |worker: Worker<'_, '_, '_, '_, (), (routing::Binding<'_>, routing::Binding<'_>), ()>| {
            worker.lend(|(relayed, direct), stage| {
                let mut driver = stage.driver();
                assert!(matches!(
                    pool::Binding::new(&pool).bind_routed(
                        relayed, capture(), direct_plan, &mut driver,
                    ),
                    Err(endpoint::BindError::Framing { plan: None, socket: Some((prefix, _)) })
                        if usize::from(prefix.extent().get()) == Ticket::LEN,
                ));
                assert!(matches!(
                    pool::Binding::new(&pool).bind_routed(
                        direct, capture(), relay_plan, &mut driver,
                    ),
                    Err(endpoint::BindError::Framing { plan: Some((prefix, _)), socket: None })
                        if usize::from(prefix.extent().get()) == Ticket::LEN,
                ));
            });
        };
    let mut creation = Attempt::new();
    let executor = Executor::new(&mut creation, settings)
        .map_err(|error| format!("{:?}", error.kind()))
        .expect("driver creation");
    executor.run(|executor| {
        executor
            .enter(
                ((relayed, direct), run),
                Policy::new(
                    |_| {},
                    |failure: Recovery<'_, '_>| -> Infallible {
                        panic!("native recovery: {:?}", failure.cause())
                    },
                ),
            )
            .as_ref()
            .map_err(|error| error.cause())
            .expect("driver retirement");
    });
}

#[test]
#[ignore = "requires native reuse-port routing"]
fn owned_routed_server_starts_application() {
    let address = ([127, 0, 0, 1], 0).into();
    let count = routing::Count::try_from(2).expect("two shards");
    let mut opened = [false; 2];
    for relay in [false, true] {
        let mut group = if relay {
            routing::Group::bind_relay(address, count)
        } else {
            routing::Group::bind(address, count)
        }
        .expect("routing group");
        let mut bindings = group.split();
        let first = server::Plan::<0, receive::Copied>::routed(
            bindings.next().expect("first binding"),
            common::peer::raw(0x38)
                .expect("identity")
                .0
                .server()
                .expect("server TLS"),
            Options::default(),
            CONFIG,
        )
        .expect("first plan");
        let second = server::Plan::<1, receive::Copied>::routed(
            bindings.next().expect("second binding"),
            common::peer::raw(0x39)
                .expect("identity")
                .0
                .server()
                .expect("server TLS"),
            Options::default(),
            CONFIG,
        )
        .expect("second plan");
        let settings = Needs::from_tree(&(first.as_ref(), second.as_ref()))
            .expect("claims")
            .apply_exact(config::Config::driver(
                settings::Datagram::fixed::<2, 1200>(),
            ))
            .expect("driver configuration");
        type Plans<'g> = (
            server::Plan<'g, 0, receive::Copied, routing::Routed>,
            server::Plan<'g, 1, receive::Copied, routing::Routed>,
        );
        let run = |worker: Worker<'_, '_, '_, '_, (), Plans<'_>, ()>| {
            worker.lend(|(first, second), session| {
                let (first, second) = {
                    let mut driver = session.driver();
                    let events = capture();
                    let first = first
                        .bind_with::<app::Silent, _, _, _, Infallible>(&mut driver, move |_| {
                            Ok((events, Idle))
                        })
                        .expect("owned routed application");
                    let second = second
                        .bind(capture(), &mut driver)
                        .expect("owned routed idle");
                    (first, second)
                };
                assert_eq!(first.local_addr(), second.local_addr());
                let sender = Socket::bind(address).expect("external peer");
                let ticket = relay.then(|| {
                    Key::try_new()
                        .expect("ticket key")
                        .issue(0, sender.local_addr().expect("peer address"))
                });
                let initial = common::peer::Initial::new(
                    &[0xa5, 0xa5, 0xa5, 0xa5, 0xa5, 0xa5, 0xa5, 0xa4],
                    &[0x42; 8],
                    0,
                )
                .seal(&[0])
                .expect("external Initial");
                let wire =
                    common::runtime::relay::frame(ticket, &initial).expect("Initial framing");
                assert_eq!(
                    sender
                        .send_to(&wire, first.local_addr())
                        .expect("send external Initial"),
                    wire.len(),
                );
                let mut finish = session::Completion::new();
                let received = session
                    .with_quiesce(&mut finish, Host::new(first), TIMEOUT, |mut app| {
                        let deadline = Instant::now() + TIMEOUT;
                        while app.inspect(Host::manifold, |endpoint| endpoint.is_empty()) {
                            if Instant::now() >= deadline {
                                return false;
                            }
                            fibers::TEST
                                .try_run(&mut app, fibers::Yield::new())
                                .expect("native receive progress");
                        }
                        app.inspect(Host::manifold, |endpoint| {
                            endpoint
                                .inspect(endpoint.sole().expect("one opening"), |_, _, conn| {
                                    conn.lifecycle().is_opening()
                                })
                                .expect("live opening")
                        })
                    })
                    .expect("owned endpoint retirement");
                drop(second);
                received
            })
        };
        let mut creation = Attempt::new();
        let executor = Executor::new(&mut creation, settings)
            .map_err(|error| format!("{:?}", error.kind()))
            .expect("driver creation");
        executor.run(|executor| {
            opened[usize::from(relay)] = *executor
                .enter(
                    ((first, second), run),
                    Policy::new(
                        |_| {},
                        |failure: Recovery<'_, '_>| -> Infallible {
                            panic!("native recovery: {:?}", failure.cause())
                        },
                    ),
                )
                .as_ref()
                .map_err(|error| error.cause())
                .expect("driver retirement");
        });
    }
    assert_eq!(opened, [true, true], "direct and relayed Initial admission");
}
