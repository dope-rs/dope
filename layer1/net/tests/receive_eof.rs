pub mod common;

use std::{
    convert::Infallible,
    io::{self, Write},
    net::Shutdown,
};

use common::{
    discard::{self, eof},
    wire::Input,
};
use dope_core::{
    Errno,
    driver::{life::recovery, retained, schedule::ready},
    io::{
        Reclaimed,
        event::{
            self,
            batch::Flow,
            net::{
                accept::Failure,
                recv::{Kind, Status},
            },
        },
    },
};
use dope_net::{
    link::{
        event::{ControlRecv, Dispatch, DispatchRecv, ParkRecv, Reservation, SendOutcome},
        pool::{
            pending::Pending,
            recv::{self, input},
        },
        slot::{
            transmit::{Attempt, Finalize},
            types::{CloseGate, Demand, Readiness},
        },
    },
    wire::terminal::{Receive, Terminal},
};
use dope_test::scope;

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("driver recovery: {:?}", failure.cause())
}

fn finish(
    result: Result<Result<(), discard::Error>, scope::Error<'_, Result<(), discard::Error>>>,
) {
    result
        .as_ref()
        .map_err(scope::Error::cause)
        .expect("native lifecycle");
    result.ok().expect("driver result").expect("EOF fixture");
}

fn settled(reports: Vec<Reclaimed>) {
    assert!(
        reports.len() <= 1,
        "only the retained accept may need quiescence"
    );
    for event in reports {
        assert!(
            matches!(event, Reclaimed::Accept {
            result: Err(Failure::Syscall(error)), flow: Flow::Last, ..
        } if error.is_canceled()),
            "unsettled native operation: {event:?}"
        );
    }
}

#[test]
fn tail_keeps_cancel() {
    let (reports, input) = eof::run::<input::Scoped>(
        |pool, key, scope, mut peer| {
            let (flow, done) =
                eof::final_byte(pool, key, scope, &mut peer).expect("final byte completion");
            assert!(flow == Flow::More || std::env::consts::OS != "linux");
            let data = match pool.reserve().data(done) {
                Reservation::Ready(data) => Some(data),
                _ => None,
            }
            .expect("final reservation");
            let (seen, chunk) = match data.scoped() {
                DispatchRecv::Chunk(seen, chunk) => Some((seen, chunk)),
                _ => None,
            }
            .expect("final plaintext");
            assert_eq!(seen, key);
            assert_eq!(chunk.as_ref(), b"F");
            let address = chunk.as_ref().as_ptr();
            assert!(matches!(
                pool.recv().defer(key, chunk),
                recv::Deferral::Ready
            ));
            assert!(
                pool.inspect()
                    .get(key)
                    .expect("retained EOF owner")
                    .terminal
                    .has_eof()
            );
            eof::cancel(pool, scope, flow, |flow, status, dispatch| {
                assert_eq!(flow, Flow::Last);
                assert!(matches!(status, Status::Cancelled));
                assert!(matches!(dispatch, Dispatch::Settled));
            })
            .expect("native cancellation");
            assert!(!pool.inspect().has_rearm());
            let resumed = match pool.recv().resume(key.target()) {
                recv::Resumption::Ready(resumed) => Some(resumed),
                _ => None,
            }
            .expect("same generation");
            let mut receiver = pool.recv();
            let (seen, tail) = receiver
                .take_tail(&resumed)
                .expect("retained plaintext tail");
            assert_eq!(seen, key);
            let tail = tail.commit();
            assert_eq!(tail.as_ref(), b"F");
            assert_eq!(tail.as_ref().as_ptr(), address);
            drop(tail);
            receiver.finish_tail(&resumed);
            assert!(matches!(
                pool.get_mut(key)
                    .expect("live connection")
                    .terminal
                    .finish_recv(),
                Receive::Eof
            ));
            pool.resume(key);
            eof::arm(pool, key, scope).expect("receive admission");
            eof::flush(pool, scope).expect("receive flush");
            assert!(!pool.inspect().has_rearm());
            assert!(matches!(
                pool.get_mut(key)
                    .expect("same generation")
                    .terminal
                    .finish_recv(),
                Receive::Idle
            ));
            assert!(matches!(
                pool.inspect().get(key).expect("send remains live").demand(),
                Demand::Continue
            ));
        },
        recover,
        finish,
    );
    assert_eq!(input, Input::Final);
    settled(reports);
}

#[test]
fn credit_return_keeps_eof() {
    let (reports, input) = eof::run::<input::Retained>(
        |pool, key, scope, mut peer| {
            let (flow, done) =
                eof::final_byte(pool, key, scope, &mut peer).expect("final byte completion");
            let data = match pool.reserve().data(done) {
                Reservation::Ready(data) => Some(data),
                _ => None,
            }
            .expect("credit reservation");
            let (seen, chunk) = match data.retained() {
                DispatchRecv::Chunk(seen, chunk) => Some((seen, chunk)),
                _ => None,
            }
            .expect("retained plaintext");
            assert_eq!(seen, key);
            assert_eq!(chunk.as_ref(), b"F");
            if flow == Flow::More {
                peer.write_all(b"L").expect("late native data");
                let (flow, late) = eof::receive(scope).expect("late native completion");
                assert_eq!(flow, Flow::More);
                let late = match late {
                    Kind::Data(late) => Some(late),
                    _ => None,
                }
                .expect("late native buffer");
                assert!(matches!(pool.reserve().data(late), Reservation::Drop));
            }
            eof::cancel(pool, scope, flow, |flow, status, dispatch| {
                assert_eq!(flow, Flow::Last);
                assert!(matches!(status, Status::Cancelled));
                assert!(matches!(dispatch, Dispatch::Settled));
            })
            .expect("native cancellation");
            assert!(
                pool.inspect()
                    .get(key)
                    .expect("retained EOF owner")
                    .terminal
                    .has_eof()
            );
            assert!(!pool.inspect().has_rearm());
            drop(chunk);
            let resumed = match pool.recv().resume(key.target()) {
                recv::Resumption::Ready(resumed) => Some(resumed),
                _ => None,
            }
            .expect("returned credit");
            assert_eq!(resumed.key(), key);
            assert!(matches!(
                pool.recv().pop_ready(&resumed),
                recv::Ready::Empty
            ));
            assert!(matches!(
                pool.get_mut(key)
                    .expect("live EOF owner")
                    .terminal
                    .finish_recv(),
                Receive::Eof
            ));
            pool.resume(key);
            eof::arm(pool, key, scope).expect("receive admission");
            eof::flush(pool, scope).expect("receive flush");
            assert!(!pool.inspect().has_rearm());
            let slot = pool
                .get_mut(key)
                .expect("same generation after credit return");
            assert!(!slot.is_closing());
            assert!(matches!(slot.demand(), Demand::Continue));
            assert!(matches!(slot.terminal.finish_recv(), Receive::Idle));
            slot.close_after();
            assert!(matches!(
                slot.close_due(CloseGate::Deferred),
                Readiness::Waiting
            ));
            assert!(matches!(
                slot.close_due(CloseGate::Immediate),
                Readiness::Due
            ));
        },
        recover,
        finish,
    );
    assert_eq!(input, Input::Final);
    settled(reports);
}

#[test]
fn closing_settles_eof() {
    let (reports, input) = eof::run::<input::Scoped>(
        |pool, key, scope, peer| {
            eof::arm(pool, key, scope).expect("receive admission");
            pool.get_mut(key).expect("live connection").io().close();
            assert!(Pending::of(pool).is_empty());
            peer.shutdown(Shutdown::Write).expect("peer FIN");
            let (flow, item) = eof::receive(scope).expect("native EOF");
            assert_eq!(flow, Flow::Last);
            let done = match item {
                Kind::Control(done) => Some(done),
                _ => None,
            }
            .expect("EOF completion");
            assert!(matches!(done.status(), Status::Eof));
            assert!(matches!(pool.recv().dispatch(done), Dispatch::Settled));
            assert!(!Pending::of(pool).is_empty());
            assert!(!pool.inspect().has_rearm());
            assert!(matches!(
                pool.get_mut(key)
                    .expect("closing owner")
                    .terminal
                    .finish_recv(),
                Receive::Idle
            ));
        },
        recover,
        finish,
    );
    assert_eq!(input, Input::Empty);
    settled(reports);
}

#[test]
fn shutdown_wakes_terminal() {
    let (reports, input) = eof::run::<input::Scoped>(
        |pool, key, scope, peer| {
            eof::arm(pool, key, scope).expect("receive admission");
            peer.shutdown(Shutdown::Write).expect("peer FIN");
            let (flow, item) = eof::receive(scope).expect("native EOF");
            assert_eq!(flow, Flow::Last);
            let input = match item {
                Kind::Control(done) => Some(done),
                _ => None,
            }
            .expect("EOF completion");
            {
                pool.get_mut(key).expect("live connection").shutdown();
                let mut driver = scope.context();
                let owner = retained::raw::Owner::from_driver(driver.driver_ref());
                let mut driver = retained::Context::new(driver.reborrow(), owner);
                assert!(matches!(
                    pool.sender(key).expect("send owner").submit(&mut driver),
                    Attempt::Waiting
                ));
            }
            let output = match discard::next(scope).expect("native shutdown").into_kind() {
                event::Kind::Send(done) => Some(done),
                _ => None,
            }
            .expect("shutdown completion");
            assert!(matches!(pool.recv().dispatch(input), Dispatch::Ready(_)));
            assert!(
                pool.get_mut(key)
                    .expect("terminal owner")
                    .terminal
                    .has_send()
            );
            assert!(matches!(
                eof::take_ready(scope, |token| {
                    assert!(key.target().dispatch().matches(token));
                }),
                ready::Readiness::Ready
            ));
            assert!(matches!(
                eof::end(pool, key, scope).expect("terminal send owner"),
                Finalize::Armed(())
            ));
            assert!(matches!(
                eof::take_ready(scope, |token| {
                    assert!(key.target().dispatch().matches(token));
                }),
                ready::Readiness::Idle
            ));
            {
                let mut driver = scope.context();
                let owner = retained::raw::Owner::from_driver(driver.driver_ref());
                let mut driver = retained::Context::new(driver.reborrow(), owner);
                assert!(
                    matches!(pool.settle(output).settle_owned(&mut driver, |_, _| {}), SendOutcome::WriteShutdown { key: seen } if seen == key)
                );
            }
            assert!(matches!(
                eof::take_ready(scope, |token| {
                    assert!(key.target().dispatch().matches(token));
                }),
                ready::Readiness::Ready
            ));
            assert!(matches!(
                eof::end(pool, key, scope).expect("terminal send owner"),
                Finalize::Settled
            ));
            let slot = pool.get_mut(key).expect("same terminal owner");
            let error = match slot.terminal.finish_recv() {
                Receive::Ready(Terminal::Failed(error)) => Some(error),
                _ => None,
            }
            .expect("original terminal failure");
            assert_eq!(error.into_parts(), (7, None));
            assert!(matches!(slot.terminal.finish_recv(), Receive::Idle));
        },
        recover,
        finish,
    );
    assert_eq!(input, Input::Empty);
    settled(reports);
}

#[test]
fn closing_wakes_exhaustion() {
    let (mut reports, input) = eof::run::<input::Scoped>(
        |pool, key, scope, mut peer| {
            eof::arm(pool, key, scope).expect("receive admission");
            pool.suspend(key);
            for _ in 0..2 {
                peer.write_all(b"F").expect("parked native byte");
                let (flow, item) = eof::receive(scope).expect("native byte completion");
                assert_eq!(flow, Flow::More);
                let done = match item {
                    Kind::Data(done) => Some(done),
                    _ => None,
                }
                .expect("native buffer");
                assert!(matches!(
                    pool.reserve().data(done),
                    Reservation::Parked(ParkRecv::Deferred)
                ));
            }
            peer.write_all(b"F").expect("exhaust receive buffers");
            let (flow, item) = eof::receive(scope).expect("native exhaustion");
            assert_eq!(flow, Flow::Last);
            let done = match item {
                Kind::Control(done) => Some(done),
                _ => None,
            }
            .expect("terminal completion");
            assert!(matches!(done.status(), Status::Exhausted));
            pool.get_mut(key).expect("closing owner").io().close();
            assert!(Pending::of(pool).is_empty());
            let done = match pool.recv().dispatch(done) {
                Dispatch::Retired(done) => Some(done),
                _ => None,
            }
            .expect("original exhaustion authority");
            assert!(!Pending::of(pool).is_empty());
            scope.context().reclaim(done);
            pool.resume(key);
            let resumed = match pool.recv().resume(key.target()) {
                recv::Resumption::Ready(resumed) => Some(resumed),
                _ => None,
            }
            .expect("same receive generation");
            for _ in 0..2 {
                let done = match pool.recv().pop_ready(&resumed) {
                    recv::Ready::Completion(done) => Some(done),
                    _ => None,
                }
                .expect("parked input owner");
                drop(done);
            }
            assert!(matches!(
                pool.recv().pop_ready(&resumed),
                recv::Ready::Empty
            ));
            eof::flush(pool, scope).expect("receive flush");
            assert!(!pool.inspect().has_rearm());
            assert!(matches!(
                pool.get_mut(key)
                    .expect("closing owner")
                    .terminal
                    .finish_recv(),
                Receive::Idle
            ));
        },
        recover,
        finish,
    );
    let index = reports.iter().position(|event| matches!(event,
        Reclaimed::RecvRoute { result: Err(error), flow: Flow::Last, .. } if *error == Errno::NO_BUFFER_SPACE
    )).expect("exact exhausted completion");
    reports.remove(index);
    assert_eq!(input, Input::Empty);
    settled(reports);
}

#[test]
fn closing_wakes_reset() {
    let (reports, input) = eof::run::<input::Scoped>(
        |pool, key, scope, peer| {
            eof::arm(pool, key, scope).expect("receive admission");
            pool.get_mut(key).expect("closing owner").io().close();
            assert!(Pending::of(pool).is_empty());
            dope_test::harness::peer::Peer::try_reset(peer).expect("peer reset");
            let (flow, item) = eof::receive(scope).expect("native failure");
            assert_eq!(flow, Flow::Last);
            let done = match item {
                Kind::Control(done) => Some(done),
                _ => None,
            }
            .expect("failed completion");
            let error = match done.status() {
                Status::Failed(error) => Some(error),
                _ => None,
            }
            .expect("native reset error");
            assert_eq!(
                io::Error::from_raw_os_error(error.code().get()).kind(),
                io::ErrorKind::ConnectionReset
            );
            assert!(matches!(
                pool.recv().dispatch(done),
                Dispatch::Ready(ControlRecv::Failed { key: seen, error: cause })
                    if seen == key && cause == error
            ));
            assert!(!Pending::of(pool).is_empty());
            assert!(!pool.inspect().has_rearm());
        },
        recover,
        finish,
    );
    assert_eq!(input, Input::Empty);
    settled(reports);
}
