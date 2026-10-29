pub mod common;

use std::{
    os::{fd::AsFd as _, unix::net},
    thread,
};

use dope_sys::{
    Errno,
    io::{Close as _, Retry as _, reactor::submit},
    kernel, probe,
    ring::{
        self, Access as _, Owner as _, create,
        enter::Enter as _,
        map::{self, Unmap as _},
    },
    table::token,
};
use dope_uring::Uring;

use crate::common::{
    cleanup::{
        self, Host,
        schedule::{self, Error, Schedule},
    },
    receive::Layout,
    ring::{Ring as _, seccomp},
};

type Token = <Uring as token::Source>::Token<Host>;
type Ring = <Uring as ring::Source>::Ring<Token>;
type Waiting = <Uring as probe::Source>::Waiting<Token, Ring>;
type Primary = <Uring as create::Source>::Primary;
type Recover = <Uring as probe::Source>::Cleanup<Token, Layout, u64>;
type Cleanup = <Uring as probe::Source>::Cleanup<Token, Layout, Primary>;
type Resources = <Uring as create::Source>::Resources<Token, Layout, Primary>;
type Target = <Uring as submit::Control>::Target;
type Released = <Uring as probe::Source>::Release<Token, Layout, u64>;

#[test]
fn retains_setup_primary() {
    use dope_sys::memory::Cleanup as _;

    use crate::common::ring::Ring as _;

    let mut observed = Vec::new();
    let faults = Schedule::new(schedule::ERRORS)
        .try_run(|| {
            let error = Uring::try_ring::<8, 4>(false)
                .expect_err("completion capacity below submission capacity");
            let state = match error {
                create::Error::Cleanup(state) => Some(state),
                _ => None,
            }
            .expect("setup failure retains its backing cleanup");
            let primary = state.primary();
            assert!(
                matches!(primary, create::Cause::Setup(error) if error.code().get() == libc::EINVAL)
            );
            let first = state.failure();
            assert_eq!(state.failure().into_parts().2.code().get(), libc::ENOMEM);
            let state = state
                .try_close(|cause| observed.push(cause))
                .expect_err("second backing unmap failure");
            assert!(state.primary() == primary);
            let second = state.failure();
            assert_eq!(state.failure().into_parts().2.code().get(), libc::EAGAIN);
            assert!(
                state
                    .try_close(|cause| observed.push(cause))
                    .is_ok_and(|value| value == primary)
            );
            assert_eq!(observed, [first, second]);
        })
        .expect("two injected backing unmaps");
    assert_eq!(faults[0].address, faults[1].address);
    assert_eq!(faults[0].length, faults[1].length);
}

#[test]
fn retains_probe_primary() {
    use dope_sys::probe::{self, cleanup::Cleanup as _};

    let ring = cleanup::try_ring().expect("cleanup ring setup");
    let mut observed = Vec::new();
    let faults = Schedule::new(schedule::ERRORS)
        .try_run(|| {
            let state = Recover::raw(ring, 41).expect_err("first probe unmap failure");
            assert_eq!(state.primary(), 41);
            let first = state.failure();
            assert!(matches!(first, probe::cleanup::Failure::Storage(_)));
            let state = state
                .try_close(|cause| observed.push(cause))
                .expect_err("second probe unmap failure");
            assert_eq!(state.primary(), 41);
            let second = state.failure();
            assert!(second != first);
            assert!(matches!(
                state.try_close(|cause| observed.push(cause)),
                Ok(41)
            ));
            assert_eq!(observed, [first, second]);
        })
        .expect("two injected unmaps");
    assert_eq!(faults[0].address, faults[1].address);
    assert_eq!(faults[0].length, faults[1].length);
    assert_eq!(
        [faults[0].code.get(), faults[1].code.get()],
        [libc::ENOMEM, libc::EAGAIN]
    );
}

#[test]
fn retains_resource_primary() {
    use dope_sys::{
        memory::{self, Cleanup as _},
        probe::cleanup::Cleanup as _,
        ring::create::{self, Retain as _},
    };

    let ring = cleanup::try_ring().expect("cleanup ring setup");
    let primary = Primary::Memory(memory::Error::Length {
        element: 41,
        count: 43,
    });
    let mut observed = Vec::new();
    let faults = Schedule::new(schedule::ERRORS)
        .try_run(|| {
            let state = Cleanup::raw(ring, primary).expect_err("first resource unmap failure");
            let state = Resources::probe(state);
            assert!(state.primary() == primary);
            let first = state.failure();
            assert!(matches!(first, create::Failure::Storage(_)));
            let state = state
                .try_close(|cause| observed.push(cause))
                .expect_err("second resource unmap failure");
            assert!(state.primary() == primary);
            let second = state.failure();
            assert!(second != first);
            assert!(
                state
                    .try_close(|cause| observed.push(cause))
                    .is_ok_and(|value| value == primary)
            );
            assert_eq!(observed, [first, second]);
        })
        .expect("two injected unmaps");
    assert_eq!(faults[0].address, faults[1].address);
    assert_eq!(faults[0].length, faults[1].length);
    assert_eq!(
        [faults[0].code.get(), faults[1].code.get()],
        [libc::ENOMEM, libc::EAGAIN]
    );
}

#[test]
fn retains_removed_slot() {
    use dope_sys::{
        io::reactor::submit::Target as _,
        memory::Cleanup as _,
        probe::cleanup::Release as _,
        ring::{Access as _, register::Registry as _},
    };
    use o3::base::num::bounded;

    let mut ring = cleanup::try_ring().expect("cleanup ring setup");
    ring.registry()
        .sparse_files(bounded::U32::<1, { u32::MAX }>::exact::<2>())
        .expect("removal file table");
    let sockets = net::UnixStream::pair().expect("removal socket pair");
    let slot = Target::from_le([0; 3]);
    let file = ring
        .registry()
        .occupy(slot, sockets.0.as_fd())
        .expect("occupied removal slot");
    let mut observed = Vec::new();
    let faults = Schedule::new(schedule::ERRORS)
        .try_run(|| {
            let state = Released::raw(ring, file, 41).expect_err("first removal unmap failure");
            assert!(state.primary() == (41, slot));
            let first = state.failure();
            let state = state
                .try_close(|cause| observed.push(cause))
                .expect_err("second removal unmap failure");
            assert!(state.primary() == (41, slot));
            let second = state.failure();
            assert!(second != first);
            assert!(
                state
                    .try_close(|cause| observed.push(cause))
                    .is_ok_and(|value| value == (41, slot))
            );
            assert_eq!(observed, [first, second]);
        })
        .expect("two injected unmaps");
    assert_eq!(faults[0].address, faults[1].address);
    assert_eq!(faults[0].length, faults[1].length);
    assert_eq!(
        [faults[0].code.get(), faults[1].code.get()],
        [libc::ENOMEM, libc::EAGAIN]
    );
}

#[test]
fn reject_keeps() {
    use dope_sys::{
        io::{
            Close as _,
            reactor::{
                complete::{Head as _, Inspect as _, Observe, Record},
                submit::Control as _,
            },
        },
        probe::{self, raw::Wait as _},
        ring::Complete as _,
        table::token::Codec as _,
    };

    let ring = cleanup::try_ring().expect("cleanup ring setup");
    let entry = Uring::nop(Token::probe(8));
    let waiting = Waiting::submit(ring, |ring| ring, &entry, 7).expect("probe submission");
    let stopped = waiting
        .finish(|ring| ring)
        .err()
        .expect("unexpected probe identity");
    let mut ring = stopped.state;
    let mut queue = ring.completion();
    assert_eq!(queue.available().get(), 1);
    let head = queue
        .front()
        .expect("rejected completion remains owned by the queue");
    let parts = head.parts();
    let observed =
        <Uring as Observe<Token>>::Observation::new(parts.token, parts.outcome, parts.flags);
    assert!(
        matches!(stopped.cause, probe::Cause::Unexpected { probe: 7, observed: rejected } if rejected == observed)
    );
    assert!(parts.token == Token::probe(8));
    head.commit();
    assert_eq!(queue.available().get(), 0);
    assert!(ring.try_close().is_ok());
}

#[test]
fn resume_probe() {
    use dope_sys::{
        io::{
            Close as _,
            reactor::submit::{Control as _, Queue as _, Ready as _, raw::Vacancy as _},
        },
        probe::{self, Outcome as _, raw::Wait as _},
        ring::Access as _,
        table::token::Codec as _,
    };

    let mut ring = cleanup::try_ring().expect("cleanup ring setup");
    ring.sq()
        .try_acquire()
        .expect("foreign probe vacancy")
        .prepare(&Uring::nop(Token::probe(8)))
        .push();
    let waiting = Waiting::submit(ring, |ring| ring, &Uring::nop(Token::probe(7)), 7)
        .expect("expected probe submission");
    let stopped = waiting
        .next(|ring| ring)
        .err()
        .expect("foreign head is retained");
    let waiting = match stopped.state {
        probe::State::Active(waiting) => Some(waiting),
        probe::State::Idle(_) => None,
    }
    .expect("pending probe keeps its owner");
    let cause = stopped.cause;
    assert!(matches!(cause, probe::Cause::Unexpected { probe: 7, .. }));
    let mut reported = Vec::new();
    let resumed = Waiting::resume(
        probe::Stopped {
            state: waiting,
            cause,
        },
        |ring| ring,
        |cause| reported.push(cause),
    );
    assert!(
        resumed.is_ok(),
        "reported head can be acknowledged: {:?}",
        resumed.as_ref().err().map(|stopped| &stopped.cause),
    );
    if let Ok(waiting) = resumed {
        assert!(reported == [cause]);
        let (ring, done) = waiting.finish(|ring| ring).expect("next probe completes");
        assert!(done.zero().is_ok());
        assert!(ring.try_close().is_ok());
    }
}

#[test]
fn keeps_next() {
    use dope_sys::{
        io::{Close as _, reactor::submit::Control as _},
        probe::{self, Outcome as _, raw::Wait as _},
        ring::{Access as _, enter::Enter as _},
        table::token::Codec as _,
    };

    let ring = cleanup::try_ring().expect("cleanup ring setup");
    let entry = Uring::nop(Token::probe(7));
    let first = Waiting::submit(ring, |ring| ring, &entry, 7).expect("first probe submission");
    let (ring, done) = first
        .finish(|ring| ring)
        .expect("first completion consumed");
    let cause = probe::Cause::Check(
        done.canceled()
            .expect_err("the consumed NOP completion is not a cancellation"),
    );
    let waiting = Waiting::submit(ring, |ring| ring, &entry, 7).expect("second probe submission");
    let checked = unsafe { waiting.inspect(|ring| ring.enter().try_wait(None)) };
    assert!(
        checked.is_ok(),
        "second completion is published without consuming its head: {:?}",
        checked.as_ref().err().map(|stopped| &stopped.cause),
    );
    if let Ok(waiting) = checked {
        let mut reported = Vec::new();
        let resumed = Waiting::resume(
            probe::Stopped {
                state: waiting,
                cause,
            },
            |ring| ring,
            |cause| reported.push(cause),
        );
        assert!(
            resumed.is_ok(),
            "a consumed check needs no acknowledgement: {:?}",
            resumed.as_ref().err().map(|stopped| &stopped.cause),
        );
        if let Ok(waiting) = resumed {
            assert!(reported == [cause]);
            let (ring, done) = waiting
                .finish(|ring| ring)
                .expect("identical next head is retained");
            assert!(done.zero().is_ok());
            assert!(ring.try_close().is_ok());
        }
    }
}

#[test]
fn close_frees_registration() {
    for attempt in 0..128 {
        let ring = Uring::try_ring::<8, 16>(false).expect("ring fixture");
        let mut ring = ring.try_register().expect("ring registration");
        assert!(ring.enter().try_submit().is_ok());
        assert!(
            ring.try_close().is_ok(),
            "ring close failed at iteration {attempt}"
        );
    }
}

#[test]
fn release_keeps_mappings() {
    thread::spawn(|| {
        let ring = Uring::try_ring::<2, 4>(false).expect("ring fixture");
        let ring = ring.try_register().expect("ring registration");
        seccomp::deny_unregister().expect("thread-local unregister filter");
        let unmaps = Schedule::new(schedule::ERRORS).try_run(|| {
            let state = ring.try_close().expect_err("unregister denied");
            let first = state.failure();
            assert!(
                matches!(&first, map::Storage::Unregister(error) if error.code().get() == libc::EPERM)
            );
            let (owner, cause) = state.recover().into_parts();
            assert_eq!(cause, first);
            let state = owner.try_close().expect_err("unregister retry denied");
            assert_eq!(state.failure(), first);
        });
        assert!(matches!(unmaps, Err(Error::Missing([None, None]))));
    })
    .join()
    .expect("unregister owner remains on its filtered thread");
}

#[test]
fn mapping_retry_stays_free() {
    thread::spawn(|| {
        let ring = Uring::try_ring::<2, 4>(false).expect("ring fixture");
        let ring = ring.try_register().expect("ring registration");
        let mut observed = Vec::new();
        let unmaps = Schedule::new(schedule::ERRORS)
            .try_run(|| {
                let state = ring.try_close().expect_err("first mapping failure");
                let first = state.failure();
                let (owner, cause) = state.recover().into_parts();
                assert_eq!(cause, first);
                observed.push(first);
                seccomp::deny_unregister().expect("thread-local unregister filter");
                let state = owner.try_close().expect_err("second mapping failure");
                let second = state.failure();
                let (owner, cause) = state.recover().into_parts();
                assert_eq!(cause, second);
                observed.push(second);
                assert!(owner.try_close().is_ok(), "mapping cleanup completes");
            })
            .expect("two mapping failures after registration release");
        for (failure, unmap) in observed.into_iter().zip(unmaps) {
            let error = Errno::try_from(unmap.code).expect("fixture errno");
            assert_eq!(
                failure,
                map::Storage::Ring(map::Failure::new(
                    unmap.address,
                    std::num::NonZero::new(unmap.length).expect("mapping length"),
                    kernel::Errno::from(error),
                )),
            );
        }
    })
    .join()
    .expect("mapping owner remains on its filtered thread");
}

#[test]
fn retirement_wait_errors() {
    use dope_sys::{
        engine::{life, life::Quiesced as _},
        ring::enter::Poll,
    };

    use crate::common::cleanup::backend;

    let denied = Errno::try_from(libc::EPERM).expect("positive errno");
    for code in [Errno::INTERRUPTED, denied] {
        for flight in [true, false] {
            thread::spawn(move || {
                assert!({
                    let mut outcome = Ok(());
                    backend::with(&mut outcome, |owner| {
                        seccomp::deny_wait(code).expect("thread-local wait filter");
                        let resolver = backend::Retirement {
                            owner,
                            visits: 0,
                            flight,
                            reactor: None,
                        };
                        match <Uring as life::Finish<'_, _>>::try_finish(resolver) {
                            Ok(done) => {
                                assert_eq!(code, Errno::INTERRUPTED);
                                let resolver = done.into_inner();
                                assert_eq!(resolver.visits, 2);
                                assert!(resolver.reactor.is_none());
                            }
                            Err(stopped) => {
                                assert_eq!(code, denied);
                                assert_eq!(stopped.state.visits, 1);
                                assert!(stopped.state.reactor.is_none());
                                assert!(matches!(stopped.cause,
                                life::Error::File(Poll::Wait(actual)) if actual.is_code(code)));
                            }
                        }
                    });
                    outcome.is_ok()
                });
            })
            .join()
            .expect("retirement keeps its filtered native owner");
        }
    }
}
