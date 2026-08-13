use std::{io::ErrorKind, time::Duration};

use dope_core::{
    driver::{
        self, ops,
        route::{self, Epoch, KeyTag, SlotIndex, Target, table::Parts},
        schedule, settings,
    },
    io::{self, socket},
};
use dope_test::scenario::rt::Runtime;

const ROUTE: u8 = 7;

fn target<'d>(driver: driver::Reference<'d>, epoch: Epoch) -> Target<'d, KeyTag<ROUTE>> {
    target_at(driver, SlotIndex::ZERO, epoch)
}

fn target_at<'d>(
    driver: driver::Reference<'d>,
    slot: SlotIndex,
    epoch: Epoch,
) -> Target<'d, KeyTag<ROUTE>> {
    driver
        .targets::<KeyTag<ROUTE>>()
        .bind_parts(Parts::from_components(slot, epoch))
}

fn stream() -> socket::StreamSpec {
    let peer = socket::Addr::from_std("127.0.0.1:9".parse().expect("test address"));
    socket::StreamSpec::for_peer(&peer).expect("TCP stream socket")
}

fn wait<'d>(
    driver: &mut impl ops::poll::Source<'d>,
    turn: &mut schedule::ActiveTurn<'_, 'd>,
) -> usize {
    ops::poll::Poll::wait(driver, turn.reactor(), Some(Duration::from_secs(1)))
        .expect("wait for file completion");
    let mut count = 0;
    let _ = crate::dispatch_all(driver, turn.reactor(), |_| count += 1);
    count
}

fn wait_socket<'d>(
    driver: &mut impl ops::poll::Source<'d>,
    turn: &mut schedule::ActiveTurn<'_, 'd>,
    owner: Target<'d, KeyTag<ROUTE>>,
) -> io::event::creation::Completion<'d> {
    ops::poll::Poll::wait(driver, turn.reactor(), Some(Duration::from_secs(1)))
        .expect("wait for socket creation");
    let mut event = None;
    let _ = crate::dispatch_all(driver, turn.reactor(), |completion| {
        assert!(event.replace(completion).is_none());
    });
    let event = event.expect("socket creation completion").into_kind();
    let io::event::Kind::Socket(completion) = event else {
        panic!("expected socket creation completion");
    };
    assert!(
        owner
            .operation(route::kind::SOCKET)
            .matches(completion.token()),
        "socket completion must retain its exact routed owner",
    );
    completion
}

#[test]
fn bound_outbound_slot_is_exclusive_until_released() {
    Runtime::quic(2, 8).with_driver_scope(|scope| {
        scope.with_turn(|_, mut driver, _| {
            let reservation = ops::Files::reserve_outbound::<ROUTE>(&mut driver, 1)
                .expect("reserve one fixed slot");
            let owner = target(driver.driver_ref(), Epoch::INITIAL);
            let slot = reservation.bind(owner).expect("bind outbound slot");
            assert!(reservation.bind(owner).is_none());
            drop(slot);
            let rebound = reservation
                .bind(owner)
                .expect("released slot must be reusable");
            drop(rebound);
            ops::Files::retire_outbound(&mut driver, reservation);
        });
    });
}

#[test]
fn created_slot_rejects_a_different_creating_authority_without_consuming_either() {
    if std::env::consts::OS != "linux" {
        return;
    }
    Runtime::quic(2, 8).with_driver_scope(|scope| {
        scope.with_turn(|_, mut driver, mut controller| {
            let mut turn = controller.begin(schedule::MAX_TURN_WORK_BUDGET);
            let flights = driver
                .flight_slots::<KeyTag<ROUTE>>(2)
                .expect("reserve socket flights");
            let reservation = ops::Files::reserve_outbound::<ROUTE>(&mut driver, 2)
                .expect("reserve two outbound slots");
            let first_owner = target(driver.driver_ref(), Epoch::INITIAL);
            let first = reservation.bind(first_owner).expect("bind first slot");
            let first = ops::Control::submit_socket(&mut driver, &flights, first, stream())
                .unwrap_or_else(|_| panic!("submit first socket creation"));
            let first_created = match wait_socket(&mut driver, &mut turn, first_owner)
                .into_parts()
                .1
            {
                io::SocketEvent::Created(created) => created,
                io::SocketEvent::Failed(error) => panic!("first socket creation failed: {error}"),
            };

            let second_owner =
                target_at(driver.driver_ref(), SlotIndex::from(1_u16), Epoch::INITIAL);
            let second = reservation.bind(second_owner).expect("bind second slot");
            let second = ops::Control::submit_socket(&mut driver, &flights, second, stream())
                .unwrap_or_else(|_| panic!("submit second socket creation"));
            let second_created = match wait_socket(&mut driver, &mut turn, second_owner)
                .into_parts()
                .1
            {
                io::SocketEvent::Created(created) => created,
                io::SocketEvent::Failed(error) => panic!("second socket creation failed: {error}"),
            };

            let (second, first_created) = match first_created.activate(second) {
                Err(authorities) => authorities,
                Ok(_) => panic!("different physical creation authorities must not activate"),
            };
            let first = match first_created.activate(first) {
                Ok(fd) => fd,
                Err(_) => panic!("first creation authorities must remain intact"),
            };
            let second = match second_created.activate(second) {
                Ok(fd) => fd,
                Err(_) => panic!("second creation authorities must remain intact"),
            };

            ops::Files::retire_outbound(&mut driver, reservation);
            ops::Files::close(&mut driver, first);
            ops::Files::close(&mut driver, second);
            drop(turn);
        });
    });
}

#[test]
fn dropped_creation_waits_for_its_exact_kernel_result() {
    if std::env::consts::OS != "linux" {
        return;
    }
    Runtime::quic(2, 8)
        .file_slots(settings::FileSlots::fixed::<15, 1>())
        .with_driver_scope(|scope| {
            scope.with_turn(|_, mut driver, mut controller| {
                let mut turn = controller.begin(schedule::MAX_TURN_WORK_BUDGET);
                let flights = driver
                    .flight_slots::<KeyTag<ROUTE>>(1)
                    .expect("reserve socket flight");
                let reservation = ops::Files::reserve_outbound::<ROUTE>(&mut driver, 1)
                    .expect("reserve the outbound slot");
                let owner = target(driver.driver_ref(), Epoch::INITIAL);
                let request = reservation.bind(owner).expect("bind outbound slot");
                let creating =
                    ops::Control::submit_socket(&mut driver, &flights, request, stream())
                        .unwrap_or_else(|_| panic!("submit socket creation"));

                drop(creating);
                match wait_socket(&mut driver, &mut turn, owner).into_parts().1 {
                    io::SocketEvent::Failed(error) => {
                        assert_eq!(error.raw_os_error(), Some(libc::ECANCELED));
                    }
                    io::SocketEvent::Created(_) => {
                        panic!("dropped creation authority must not receive a live socket")
                    }
                }

                ops::Files::retire_outbound(&mut driver, reservation);
                assert_eq!(
                    ops::poll::Poll::commit(&mut driver, turn.reactor()).expect("submit close"),
                    ops::poll::Commit::Drained
                );
                for _ in 0..2 {
                    assert_eq!(wait(&mut driver, &mut turn), 0);
                }
                drop(turn);
            });
        });
}

#[test]
fn stale_created_authority_attaches_late_owner_to_inflight_close() {
    if std::env::consts::OS != "linux" {
        return;
    }
    Runtime::quic(2, 8).with_driver_scope(|scope| {
        scope.with_turn(|_, mut driver, mut controller| {
            let mut turn = controller.begin(schedule::MAX_TURN_WORK_BUDGET);
            let flights = driver
                .flight_slots::<KeyTag<ROUTE>>(1)
                .expect("reserve socket flight");
            let reservation = ops::Files::reserve_outbound::<ROUTE>(&mut driver, 16)
                .expect("reserve every fixed slot");
            let owner = target(driver.driver_ref(), Epoch::INITIAL);
            let request = reservation.bind(owner).expect("bind outbound slot");
            let creating = ops::Control::submit_socket(&mut driver, &flights, request, stream())
                .unwrap_or_else(|_| panic!("submit socket creation"));
            let created = match wait_socket(&mut driver, &mut turn, owner).into_parts().1 {
                io::SocketEvent::Created(created) => created,
                io::SocketEvent::Failed(error) => panic!("socket creation failed: {error}"),
            };

            drop(created);
            drop(creating);
            ops::Files::retire_outbound(&mut driver, reservation);

            let address = "127.0.0.1:0".parse().expect("address");
            let error = ops::Bootstrap::bind_datagram_slot(&mut driver, address)
                .expect_err("late owner must remain attached until close completion");
            assert_eq!(error.kind(), ErrorKind::WouldBlock);

            assert_eq!(
                ops::poll::Poll::commit(&mut driver, turn.reactor()).expect("submit close"),
                ops::poll::Commit::Drained
            );
            let mut released = None;
            for _ in 0..4 {
                assert_eq!(wait(&mut driver, &mut turn), 0);
                match ops::Bootstrap::bind_datagram_slot(&mut driver, address) {
                    Ok((socket, _)) => {
                        released = Some(socket);
                        break;
                    }
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {}
                    Err(error) => panic!("unexpected fixed-slot allocation failure: {error}"),
                }
            }
            let socket = released.expect("close completion must release the attached owner");
            ops::Files::close(&mut driver, socket);
            drop(turn);
        });
    });
}

#[test]
fn stale_created_authority_pins_the_slot_after_close_completion() {
    if std::env::consts::OS != "linux" {
        return;
    }
    Runtime::quic(2, 8).with_driver_scope(|scope| {
        scope.with_turn(|_, mut driver, mut controller| {
            let mut turn = controller.begin(schedule::MAX_TURN_WORK_BUDGET);
            let flights = driver
                .flight_slots::<KeyTag<ROUTE>>(1)
                .expect("reserve socket flight");
            let reservation = ops::Files::reserve_outbound::<ROUTE>(&mut driver, 1)
                .expect("reserve one fixed slot");
            let owner = target(driver.driver_ref(), Epoch::INITIAL);
            let request = reservation.bind(owner).expect("bind outbound slot");
            let creating = ops::Control::submit_socket(&mut driver, &flights, request, stream())
                .unwrap_or_else(|_| panic!("submit socket creation"));
            let created = match wait_socket(&mut driver, &mut turn, owner).into_parts().1 {
                io::SocketEvent::Created(created) => created,
                io::SocketEvent::Failed(error) => panic!("socket creation failed: {error}"),
            };

            drop(creating);
            assert_eq!(
                ops::poll::Poll::commit(&mut driver, turn.reactor()).expect("submit close"),
                ops::poll::Commit::Drained
            );
            assert_eq!(wait(&mut driver, &mut turn), 0);
            assert_eq!(wait(&mut driver, &mut turn), 0);
            assert!(
                reservation.bind(owner).is_none(),
                "a stale creation proof must pin its exact slot"
            );

            drop(created);
            assert_eq!(
                ops::poll::Poll::commit(&mut driver, turn.reactor())
                    .expect("release stale creation proof"),
                ops::poll::Commit::Drained
            );
            let rebound = reservation
                .bind(owner)
                .expect("releasing both affine halves must release the slot");
            drop(rebound);
            ops::Files::retire_outbound(&mut driver, reservation);
            drop(turn);
        });
    });
}

#[test]
fn retiring_outbound_slots_are_not_reissued_before_close_completion() {
    if std::env::consts::OS != "linux" {
        return;
    }
    Runtime::quic(2, 8).with_driver_scope(|scope| {
        scope.with_turn(|_, mut driver, mut controller| {
            let mut turn = controller.begin(schedule::MAX_TURN_WORK_BUDGET);
            let flights = driver
                .flight_slots::<KeyTag<ROUTE>>(1)
                .expect("reserve socket flight");
            let reservation = ops::Files::reserve_outbound::<ROUTE>(&mut driver, 16)
                .expect("reserve every fixed slot");
            let owner = target(driver.driver_ref(), Epoch::INITIAL);
            let request = reservation.bind(owner).expect("bind outbound slot");
            let creating = ops::Control::submit_socket(&mut driver, &flights, request, stream())
                .unwrap_or_else(|_| panic!("submit socket creation"));
            let completion = wait_socket(&mut driver, &mut turn, owner);
            let fd = crate::activate(creating, owner, completion);

            ops::Files::retire_outbound(&mut driver, reservation);
            ops::Files::close(&mut driver, fd);

            let error = ops::Bootstrap::bind_datagram_slot(
                &mut driver,
                "127.0.0.1:0".parse().expect("address"),
            )
            .expect_err("closing outbound slots must not be reissued");
            assert_eq!(error.kind(), ErrorKind::WouldBlock);

            assert_eq!(
                ops::poll::Poll::commit(&mut driver, turn.reactor()).expect("submit close"),
                ops::poll::Commit::Drained
            );
            let address = "127.0.0.1:0".parse().expect("address");
            let mut released = None;
            for _ in 0..4 {
                assert_eq!(wait(&mut driver, &mut turn), 0);
                match ops::Bootstrap::bind_datagram_slot(&mut driver, address) {
                    Ok((socket, _)) => {
                        released = Some(socket);
                        break;
                    }
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {}
                    Err(error) => panic!("unexpected fixed-slot allocation failure: {error}"),
                }
            }
            let socket = released.expect("completed outbound close must release its fixed slots");
            ops::Files::close(&mut driver, socket);
            drop(turn);
        });
    });
}
