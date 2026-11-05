pub mod common;

use std::{convert::Infallible, num::NonZero};

use dope::core::driver::{
    life::recovery::{Policy, Recovery},
    settings::{self, claim::Needs},
};
use dope::core::io::datagram::payload;
use dope_quic::{
    conn::life::{accept, options::Options, tls::servers},
    runtime::{
        endpoint::{self, config, owner::pool, server},
        mux::{app, builder},
        receive, routing,
    },
    security::relay::Ticket,
};
use dope_test::scope::Driver;

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
    let authority = accept::Authority::new(identity.server().expect("server"), Options::default())
        .expect("authority");
    let pool = servers::Pool::<0>::new(authority, NonZero::<usize>::MIN);
    let address = ([127, 0, 0, 1], 0).into();
    let bind = routing::Single::new(address);
    let direct = endpoint::Plan::<0, receive::Copied>::new(CONFIG).expect("direct plan");
    let relay = endpoint::Plan::<0, receive::Copied>::single(CONFIG, &bind).expect("relay plan");
    let settings = Needs::from_tree(&relay)
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
            let bind = routing::Single::new(address);
            let plan =
                endpoint::Plan::<0, receive::Copied>::single(CONFIG, &bind).expect("matching plan");
            let endpoint = pool::Binding::new(&pool)
                .bind_single(bind, capture(), plan, &mut driver)
                .expect("unsharded native UDP binding");
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
#[ignore = "requires CAP_BPF for the native reuse-port selector"]
fn routed_binding_rejects_mismatched_framing() {
    let (identity, _) = common::peer::raw(0x37).expect("identity");
    let authority = accept::Authority::new(identity.server().expect("server"), Options::default())
        .expect("authority");
    let pool = servers::Pool::<0>::new(authority, NonZero::<usize>::MIN);
    let address = ([127, 0, 0, 1], 0).into();
    let count = routing::Count::try_from(2).expect("two shards");
    let relayed = routing::Group::bind_relay(address, count)
        .expect("relay routing group")
        .split()
        .next()
        .expect("relay shard binding");
    let direct = routing::Group::bind(address, count)
        .expect("direct routing group")
        .split()
        .next()
        .expect("direct shard binding");
    let direct_plan = endpoint::Plan::<0, receive::Copied>::new(CONFIG).expect("direct plan");
    let relay_plan =
        endpoint::Plan::<0, receive::Copied>::routed(CONFIG, &relayed).expect("routed relay plan");
    let settings = Needs::from_tree(&relay_plan)
        .expect("claims")
        .apply_exact(config::Config::driver(
            settings::Datagram::fixed::<2, 1200>(),
        ))
        .expect("driver configuration");
    Driver::new(settings).with_driver(
        |mut driver| {
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
#[ignore = "requires CAP_BPF for the native reuse-port selector"]
fn owned_routed_server_starts_application() {
    let address = ([127, 0, 0, 1], 0).into();
    let count = routing::Count::try_from(2).expect("two shards");
    for relay in [false, true] {
        let group = if relay {
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
        Driver::new(settings).with_driver(
            |mut driver| {
                let events = capture();
                let first = first
                    .bind_with::<app::Silent, _, _, _, Infallible>(&mut driver, move |_| {
                        Ok((events, Idle))
                    })
                    .expect("owned routed application");
                let second = second
                    .bind(capture(), &mut driver)
                    .expect("owned routed idle");
                assert_eq!(first.local_addr(), second.local_addr());
                assert_ne!(first.local_addr().port(), 0);
                assert!(first.is_empty());
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
}
