pub mod common;

use common::{discard::eof, wire::Input};
use dope_core::io::event::{
    batch::Flow,
    net::recv::{Kind, Status},
};
use dope_net::link::{
    event::{self, Dispatch, ParkError, ParkRecv, Reservation},
    pool::recv::{self, input},
};

#[test]
fn full_returns_exhausted() {
    let (reports, input) = eof::run::<input::Scoped>(|pool, key, scope, peer| {
        eof::arm(pool, key, scope).expect("receive admission");
        pool.suspend(key);
        for _ in 0..2 {
            peer.write_all(b"F").expect("parked native byte");
            let (flow, item) = eof::receive(scope).expect("native data");
            assert_eq!(flow, Flow::More);
            let done = match item {
                Kind::Data(done) => Some(done),
                _ => None,
            }
            .expect("native data owner");
            assert!(matches!(
                pool.recv().data(done, |_, _| ()),
                Reservation::Parked(ParkRecv::Deferred)
            ));
        }
        peer.write_all(b"F").expect("exhaust receive buffers");
        let (flow, item) = eof::receive(scope).expect("first exhaustion");
        assert_eq!(flow, Flow::Last);
        let done = match item {
            Kind::Control(done) => Some(done),
            _ => None,
        }
        .expect("exhaustion control");
        assert!(matches!(done.status(), Status::Exhausted));
        assert!(matches!(
            pool.recv().dispatch(done),
            Dispatch::Parked(ParkRecv::Deferred)
        ));
        pool.resume(key);
        eof::arm(pool, key, scope).expect("second native flight");
        let (_, item) = eof::receive(scope).expect("second exhaustion");
        let done = match item {
            Kind::Control(done) => Some(done),
            _ => None,
        }
        .expect("second control owner");
        assert!(matches!(done.status(), Status::Exhausted));
        let token = done.token();
        let (done, seen, error) = match pool.recv().dispatch(done) {
            Dispatch::Rejected {
                completion: done,
                key: seen,
                error,
            } => Some((done, seen, error)),
            _ => None,
        }
        .expect("control queue Full");
        assert_eq!(seen, key);
        assert_eq!(error, ParkError::Full);
        assert!(token.matches(done.token()));
        assert_eq!(done.flow(), Flow::Last);
        let done = match done.classify() {
            Kind::Control(done) => Some(done),
            _ => None,
        }
        .expect("returned control owner");
        assert!(matches!(done.status(), Status::Exhausted));
        scope.context().reclaim(done.rejoin());

        pool.get_mut(key).expect("live owner").io().close();
        let resumed = match pool.recv().resume(key.target()) {
            recv::Resumption::Ready(resumed) => Some(resumed),
            _ => None,
        }
        .expect("same receive owner");
        for _ in 0..2 {
            let done = match pool.recv().pop_ready(&resumed) {
                recv::Ready::Completion(done) => Some(done),
                _ => None,
            }
            .expect("queued data");
            let done = match done.classify() {
                event::Kind::Data(done) => Some(done),
                _ => None,
            }
            .expect("queued data owner");
            drop(done);
        }
        let done = match pool.recv().pop_ready(&resumed) {
            recv::Ready::Completion(done) => Some(done),
            _ => None,
        }
        .expect("queued control");
        let done = match done.classify() {
            event::Kind::Control(done) => Some(done),
            _ => None,
        }
        .expect("queued control owner");
        assert!(matches!(done.status(), Status::Exhausted));
        drop(done);
        assert!(matches!(
            pool.recv().pop_ready(&resumed),
            recv::Ready::Empty
        ));
        eof::flush(pool, scope).expect("closed receive flush");
        assert!(!pool.inspect().has_rearm());
    });
    assert_eq!(input, Input::Empty);
    assert!(
        reports.len() <= 2,
        "only the retained accept may need quiescence"
    );
    common::discard::assert_reclaimed(&reports, 1);
}
