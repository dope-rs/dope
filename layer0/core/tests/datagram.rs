use std::convert::Infallible;

use dope_core::{
    driver,
    driver::{
        life::{creation, recovery},
        route::KeyTag,
    },
    io::{fd::handles::Descriptor, socket::install::Install},
};
use dope_test::scope::Error;
pub mod common;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

use std::{net::UdpSocket, ops::ControlFlow, time::Duration};

use dope_core::{
    dope_sys::engine::admit::Cancelled,
    driver::{
        life::domain,
        result::poll,
        retained::flight,
        route::{Epoch, FrameworkTag, Tag, indices::SlotIndex, kind},
        settings,
    },
    io::{
        datagram, event,
        event::{batch, net::recv},
    },
};
use dope_test::{alloc::Tracker, reclaim::Reports};

use crate::common::rt::Runtime;

#[test]
fn keeps_next_datagram() {
    type Route = KeyTag<7>;
    let config = Runtime::quic::<2, 8>()
        .config()
        .with_domains(settings::Domain::ONE);
    let driver = {
        let mut creation = creation::Attempt::new();
        let outcome = creation.try_driver(config);
        outcome.ok().expect("test operation completed")
    };

    {
        let mut reports = Reports::<_>::new(config, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

        let mut domain = domain::Domain::new(driver);
        let outcome = domain.start().expect("fresh generation").enter_with(
            (),
            |mut scope, _, _| {
                scope.with_driver(|mut context, mut schedule| {
                    let mut turn = schedule.begin();
                    let bind = "127.0.0.1:0"
                        .parse()
                        .expect("the datagram test address must be valid");
                    let (socket, address) = {
                        let outcome = Install::bind_udp(context.reborrow(), bind, common::DATAGRAM);
                        outcome.expect("datagram socket bootstrapped")
                    };
                    let peer = UdpSocket::bind("127.0.0.1:0")
                        .expect("the datagram test peer must bind its local socket");
                    let target = context
                        .driver_ref()
                        .targets::<Route>()
                        .bind(SlotIndex::ZERO, Epoch::INITIAL);
                    let route = target.operation::<kind::Recv>();
                    let slots = context
                        .storage
                        .reborrow()
                        .try_flights(flight::Need::<Route>::new(flight::Capacity::fixed::<1>()))
                        .expect("the datagram test must reserve one receive flight")
                        .slots();
                    drop(
                        slots
                            .recv_dgram(context.reborrow(), &socket, route)
                            .expect("the empty driver queue must admit the receive"),
                    );

                    let local = peer
                        .local_addr()
                        .expect("the bound datagram peer must retain its local address");
                    let mut received = [None, None];
                    for (input, output) in [b"123456789".as_slice(), b"12345678"]
                        .into_iter()
                        .zip(&mut received)
                    {
                        let (decoded, usage) = Tracker::usage(|| {
                            assert!(matches!(
                                peer.send_to(input, address),
                                Ok(sent) if sent == input.len()
                            ));
                            {
                                let outcome = driver::Context::wait(
                                    &mut context,
                                    turn.reactor(),
                                    Some(Duration::from_secs(1)),
                                );
                                assert!(outcome.is_ok(), "datagram polling completed");
                            };
                            let mut decoded = None;
                            let dispatch = driver::Context::dispatch(
                                &mut context,
                                turn.reactor(),
                                |event, _source| {
                                    let event = event;
                                    if let event::Kind::Recv(done) = event.into_kind()
                                        && let (token, flow, recv::Outcome::Data(buffer)) =
                                            done.into_parts()
                                    {
                                        assert!(route.matches(token));
                                        assert!(matches!(flow, batch::Flow::More));
                                        assert!(
                                            decoded
                                                .replace(datagram::Decoded::decode(buffer))
                                                .is_none()
                                        );
                                    }
                                    ControlFlow::Continue(())
                                },
                            );
                            assert!(matches!(
                                dispatch,
                                poll::Dispatch::Ready { retained: None, .. }
                            ));
                            decoded
                        });
                        assert_eq!(usage, (0, 0));
                        *output = decoded;
                        let valid = match input {
                            b"123456789" => matches!(output, Some(datagram::Decoded::Truncated(_))),
                            b"12345678" => matches!(output,
                                Some(datagram::Decoded::Packet { source, payload })
                                    if *source == local && payload.as_ref() == input
                            ),
                            _ => false,
                        };
                        assert!(valid, "unexpected datagram fixture");
                        assert!(matches!(
                            driver::Context::commit(&mut context, turn.reactor()),
                            Ok(poll::Commit::Drained)
                        ));
                    }
                    Descriptor::from(socket).close(&mut context);
                });
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        assert!(
            reports.events().expect("valid reclamation").is_empty(),
            "unexpected reclaimed operations: {reports:?}"
        );

        outcome.ok().expect("datagram domain completed")
    };
}

#[test]
fn retires_routed() {
    use dope_core::io;

    type Route = KeyTag<7>;
    let runtime = Runtime::quic::<2, 8>();
    let config = runtime.config();
    let mut reports = Reports::<_>::new(config, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    runtime.with_scope(
        |scope| {
            scope.with_driver(|mut context, mut schedule| {
                let mut turn = schedule.begin();
                let (socket, _) = Install::bind_udp(
                    context.reborrow(),
                    ([127, 0, 0, 1], 0).into(),
                    common::DATAGRAM,
                )
                .expect("bind receive socket");
                let target = context
                    .driver_ref()
                    .targets::<Route>()
                    .bind(SlotIndex::ZERO, Epoch::INITIAL)
                    .operation::<kind::Recv>();
                let slots = context
                    .storage
                    .reborrow()
                    .try_flights(flight::Need::<Route>::new(flight::Capacity::fixed::<1>()))
                    .expect("one receive flight")
                    .slots();
                let mut flight = slots
                    .recv_dgram(context.reborrow(), &socket, target)
                    .expect("submit receive");
                assert!(driver::Context::commit(&mut context, turn.reactor()).is_ok());
                let canceled = flight
                    .try_cancel(&mut context)
                    .expect("submit exact receive cancellation");
                assert!(matches!(canceled, Cancelled::Submitted | Cancelled::Queued));
                assert!(
                    driver::Context::wait(
                        &mut context,
                        turn.reactor(),
                        Some(Duration::from_secs(1)),
                    )
                    .is_ok()
                );
                let mut observed = 0;
                let dispatch =
                    driver::Context::dispatch(&mut context, turn.reactor(), |event, _source| {
                        let driver = _source;
                        let done = match event.into_kind() {
                            event::Kind::Recv(done) => Ok(done),
                            event => Err(event),
                        }
                        .ok()
                        .expect("receive cancellation");
                        assert!(target.matches(done.token()));
                        assert!(matches!(done.flow(), batch::Flow::Last));
                        let retired = driver.retire(done.retire());
                        assert!(matches!(retired, ControlFlow::Continue(())));
                        observed += 1;
                        ControlFlow::Continue(())
                    });
                assert!(matches!(
                    dispatch,
                    poll::Dispatch::Ready { retained: None, .. }
                ));
                assert_eq!(observed, 1);
                drop(flight);
                Descriptor::from(socket).close(&mut context);
            });
        },
        recovery::Policy::new(|report| reports.push(report), recover),
        finish,
    );
    assert_eq!(
        reports.events().expect("valid reclamation").len(),
        1,
        "{reports:?}"
    );
    let (target, result, flow) = reports
        .events()
        .expect("valid reclamation")
        .iter()
        .find_map(|report| match report {
            io::Reclaimed::RecvRoute {
                target,
                result,
                flow,
            } => Some((target, result, flow)),
            _ => None,
        })
        .expect("expected routed receive retirement");
    assert_eq!(target.route(), <Route as Tag>::ROUTE);
    assert_eq!(target.slot(), SlotIndex::ZERO);
    assert!(target.epoch() == Epoch::INITIAL);
    assert_eq!(*result, Err(dope_core::Errno::CANCELED));
    assert!(matches!(flow, batch::Flow::Last));
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native retirement: {:?}", failure.cause())
}

fn finish(result: Result<(), Error<'_, ()>>) {
    result
        .as_ref()
        .map_err(Error::cause)
        .expect("driver lifecycle");
}
