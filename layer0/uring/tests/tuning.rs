pub mod common;

use std::{
    cell::Cell,
    num::{self, NonZero},
    thread,
    time::Duration,
};

use dope_sys::{
    Errno,
    engine::ring::Backend as _,
    io::{
        Close as _,
        reactor::{
            Full,
            complete::{Drain as _, Entry as _, Outcome as _},
            submit::{self, Queue as _},
        },
        socket::option::tuning::{Occupied as _, Table as _},
    },
    ring::{
        Access as _, Complete as _, Owner as _, Source,
        enter::Enter,
        ready::Ready as _,
        register::{Registered as _, Registry as _},
    },
    table::token,
};
use dope_uring::Uring;
use o3::base::num::bounded;

use crate::common::{
    cleanup::{
        Host,
        backend::{self, occupy, queued, try_retire},
    },
    options::Options,
    ring::{Ring as _, Token, prefix},
};

type HostToken = <Uring as token::Source>::Token<Host>;
type Timeout<'a> = <<Uring as Source>::Enter<'a, Token> as Enter>::Timeout;

#[test]
fn retire_waits_terminal() {
    assert!({
        let mut outcome = Ok(());
        backend::with(&mut outcome, |backend| {
            let mut observed = Vec::new();
            occupy(backend, num::NonZero::<u64>::MIN).expect("gate state");
            assert_eq!(queued(backend), 0);
            assert!(
                try_retire(backend, &mut observed)
                    .expect("retirement observation")
                    .is_pending()
            );
            assert_eq!(queued(backend), 1);
            assert!(
                try_retire(backend, &mut observed)
                    .expect("retirement observation")
                    .is_pending()
            );
            assert_eq!(queued(backend), 1, "waiting retirement queues no duplicate");
            assert_eq!(
                observed.as_slice(),
                [],
                "cancel does not commit close authority"
            );
            backend.tuning().retry_cancel(1);
            assert!(
                try_retire(backend, &mut observed)
                    .expect("retirement observation")
                    .is_pending()
            );
            assert_eq!(queued(backend), 2, "failed cancel permits another attempt");
            assert_eq!(observed.as_slice(), []);
            backend
                .tuning()
                .bind(1)
                .expect("terminal owns target")
                .clear();
            assert!(
                try_retire(backend, &mut observed)
                    .expect("retirement observation")
                    .is_ready()
            );
            assert_eq!(queued(backend), 4, "terminal releases shutdown and close");
            assert_eq!(
                observed.as_slice(),
                [Some(1)],
                "only close consumes host authority"
            );

            let fresh = num::NonZero::new(2).expect("fresh tuning target");
            occupy(backend, fresh).expect("gate state");
            assert!(
                try_retire(backend, &mut observed)
                    .expect("retirement observation")
                    .is_pending()
            );
            assert_eq!(queued(backend), 5);
            backend.tuning().retry_cancel(1);
            assert_eq!(
                backend.tuning().bind(1).expect("fresh terminal").target(),
                fresh
            );
            assert!(
                try_retire(backend, &mut observed)
                    .expect("retirement observation")
                    .is_pending()
            );
            assert_eq!(
                queued(backend),
                6,
                "late reply permits retry without clearing"
            );
            assert_eq!(
                observed.as_slice(),
                [Some(1)],
                "late reply grants no close authority"
            );
            backend.tuning().bind(1).expect("fresh terminal").clear();
        });
        outcome.is_ok()
    });
}

#[test]
fn cancellation_keeps_owner() {
    assert!({
        let mut outcome = Ok(());
        backend::with(&mut outcome, |first| {
            assert!({
                let mut outcome = Ok(());
                backend::with(&mut outcome, |second| {
                    let mut observed = Vec::new();
                    occupy(first, num::NonZero::<u64>::MIN).expect("gate state");
                    occupy(second, num::NonZero::<u64>::MIN).expect("gate state");
                    assert!(
                        try_retire(first, &mut observed)
                            .expect("retirement observation")
                            .is_pending()
                    );
                    assert_eq!(queued(first), 1);
                    second.tuning().bind(1).expect("second terminal").clear();
                    assert!(
                        try_retire(second, &mut observed)
                            .expect("retirement observation")
                            .is_ready()
                    );
                    assert_eq!(queued(second), 2);
                    assert!(
                        try_retire(first, &mut observed)
                            .expect("retirement observation")
                            .is_pending()
                    );
                    assert_eq!(queued(first), 1, "other owner cannot clear first target");
                    assert_eq!(observed.as_slice(), [Some(1)]);
                    first.tuning().bind(1).expect("first terminal").clear();
                    assert!(
                        try_retire(first, &mut observed)
                            .expect("retirement observation")
                            .is_ready()
                    );
                    assert_eq!(queued(first), 3);
                    assert_eq!(observed.as_slice(), [Some(1), Some(1)]);
                });
                outcome.is_ok()
            });
        });
        outcome.is_ok()
    });
}

#[test]
fn full_cancel_can_retry() {
    use dope_sys::io::reactor::submit::{Control as _, Ready as _, raw::Vacancy as _};
    use token::Codec as _;

    assert!({
        let mut outcome = Ok(());
        backend::with(&mut outcome, |backend| {
            let mut observed = Vec::new();
            occupy(backend, num::NonZero::<u64>::MIN).expect("gate state");
            let nop = Uring::nop(HostToken::prepare());
            for _ in 0..16 {
                backend
                    .ring()
                    .submission()
                    .try_one()
                    .expect("fixture has sixteen slots")
                    .prepare(&nop)
                    .push();
            }
            assert_eq!(queued(backend), 16);
            assert!(
                try_retire(backend, &mut observed)
                    .expect("retirement observation")
                    .is_pending()
            );
            assert_eq!(queued(backend), 16, "full queue publishes no cancellation");
            assert_eq!(observed.as_slice(), []);
            backend
                .ring()
                .enter()
                .try_submit()
                .expect("submit fixture nops");
            backend.ring().submission().refresh();
            assert_eq!(queued(backend), 0);
            assert!(
                try_retire(backend, &mut observed)
                    .expect("retirement observation")
                    .is_pending()
            );
            assert_eq!(queued(backend), 1, "failed reservation remains retryable");
            assert_eq!(observed.as_slice(), []);
            backend
                .tuning()
                .bind(1)
                .expect("terminal owns target")
                .clear();
        });
        outcome.is_ok()
    });
}

#[test]
fn tuning_pins() {
    use std::{mem, num::NonZero};

    use dope_sys::io::socket::option::tuning::{
        self, Create as _, Occupied as _, Quiesce as _, Table as _, raw::State as _,
    };

    type Table = <Uring as tuning::Source>::Table<NonZero<u64>, 7>;

    let mut table = Table::try_new(NonZero::<u32>::MIN).expect("tuning table");
    assert!(table.is_quiescent());
    assert!(table.get(1).is_none());
    let target = NonZero::<u64>::MIN;
    let address = {
        let mut state = table.get(0).expect("allocated state");
        // SAFETY: this vacant pinned state has no submitted chain.
        let values = unsafe { state.as_mut().values(target, NonZero::<u32>::MIN) };
        values[0] = 17;
        values.as_ptr()
    };
    assert!(!table.is_quiescent());
    let mut moved = Table::try_new(NonZero::<u32>::MIN).expect("second tuning table");
    mem::swap(&mut table, &mut moved);
    assert!(table.is_quiescent());
    assert!(!moved.is_quiescent());
    let occupied = moved
        .get(0)
        .expect("moved state")
        .bind()
        .expect("live echo");
    assert_eq!(occupied.target(), target);
    occupied.clear();
    assert!(moved.is_quiescent());
    {
        let mut state = moved.get(0).expect("retained allocation");
        // SAFETY: the previous echo has completed, with no kernel chain live.
        let values = unsafe { state.as_mut().values(target, NonZero::<u32>::MIN) };
        assert_eq!(values.as_ptr(), address);
        assert_eq!(values[0], 17);
    }
    moved
        .get(0)
        .expect("live state")
        .bind()
        .expect("new echo")
        .clear();
    assert!(moved.is_quiescent());
}

#[test]
fn reserves_exact_cells() {
    use std::{
        os::{fd::AsFd as _, unix::net},
        time,
    };

    use dope_sys::io::socket::option::tuning::raw::Chain as _;
    use o3::depot::capacity;

    type Target = <Uring as submit::Control>::Target;

    let sockets = net::UnixStream::pair().expect("unix stream pair");
    let mut ring = Uring::try_ring::<8, 16>(false).expect("ring fixture");
    let slot = <Target as submit::Target>::ZERO;
    assert!(
        ring.registry()
            .sparse_files(bounded::U32::<1, { u32::MAX }>::exact::<1>())
            .is_ok()
    );
    assert!(ring.registry().install(slot, sockets.0.as_fd()).is_ok());

    for width in 1..=4 {
        let len = capacity::Limit::<4>::new(width).expect("reservation width");
        let calls = Cell::new(0);
        let mut values = [-1; 4];
        let options = |left| Options {
            calls: &calls,
            count: NonZero::new(left).expect("nonzero option count"),
        };
        {
            let vacant = ring.sq().try_reserve(len);
            assert!(vacant.is_ok());
        }
        assert!(ring.sq().is_empty());
        assert_eq!(calls.get(), 0);
        assert_eq!(values, [-1; 4]);

        let ready = ring.sq().try_reserve(len).expect("prefix capacity");
        // SAFETY: the exact cursor and stable scalar array survive every option completion.
        unsafe { ready.submit(&mut values, options(width), slot, Token(0)) };
        assert_eq!(ring.sq().len().get() as usize, width);
        assert_eq!(calls.get() as usize, width);
        for (index, value) in values.into_iter().enumerate() {
            assert_eq!(value, if index < width { index as i32 } else { -1 });
        }
        {
            let rejected = ring.sq().try_reserve(capacity::Limit::<8>::fixed::<8>());
            assert!(matches!(rejected, Err(Full)));
        }
        assert_eq!(calls.get() as usize, width);

        let deadline = time::Instant::now() + Duration::from_secs(2);
        let mut seen = 0;
        while seen != width {
            assert!(time::Instant::now() < deadline, "completion deadline");
            let timeout =
                Timeout::try_from(Duration::from_millis(100)).expect("completion wait duration");
            assert!(ring.enter().try_wait(Some(timeout)).is_ok());
            let mut queue = ring.completion();
            queue.acquire();
            while let Some(entry) = queue.pop() {
                let parts = entry.into_parts();
                let value = parts
                    .outcome
                    .bytes()
                    .expect("valid chain completion")
                    .expect("successful chain completion");
                assert_eq!(parts.token, Token(0));
                assert!(seen < width, "duplicate completion");
                let value: bounded::U32<0, { i32::MAX as u32 }> = value.into();
                assert_eq!(value.get(), 0);
                seen += 1;
            }
            queue.publish();
        }
        ring.sq().refresh();
        assert!(ring.sq().is_empty());
    }
    assert!(ring.try_close().is_ok());
}

#[test]
fn split_options_complete() {
    use dope_sys::io::socket::option::tuning::raw::Chain as _;
    use o3::depot::capacity;

    thread::spawn(|| {
        type Target = <Uring as submit::Control>::Target;

        let mut ring = Uring::try_ring::<4, 8>(false).expect("ring fixture");
        let slot = <Target as submit::Target>::ZERO;
        assert!(
            ring.registry()
                .sparse_files(bounded::U32::<1, { u32::MAX }>::exact::<1>())
                .is_ok()
        );
        let mut ring = ring.try_register().expect("registered ring");
        let calls = Cell::new(0);
        let mut values = [-1; 3];
        let ready = ring
            .sq()
            .try_reserve(capacity::Limit::<3>::fixed::<3>())
            .expect("option capacity");
        // SAFETY: the exact cursor and scalar storage remain live until all
        // three completions; the registered empty fixed slot remains unchanged.
        unsafe {
            ready.submit(
                &mut values,
                Options {
                    calls: &calls,
                    count: NonZero::<usize>::MIN.saturating_add(2),
                },
                slot,
                Token(3),
            )
        };
        ring.sq().refresh();
        // SAFETY: this thread owns registered ring zero and has published three
        // requests whose scalar storage remains live through the drain below.
        assert_eq!(
            unsafe { prefix::submit(NonZero::<u32>::MIN) }.expect("prefix enter"),
            1
        );
        ring.sq().refresh();
        assert_eq!(ring.sq().len().get(), 2);

        let mut failed = 0;
        let mut canceled = 0;
        while failed + canceled != 3 {
            let timeout = Timeout::try_from(Duration::from_secs(1)).expect("completion timeout");
            assert!(ring.enter().try_wait(Some(timeout)).is_ok());
            ring.acquire();
            while let Some(entry) = ring.pop() {
                let parts = entry.into_parts();
                assert_eq!(parts.token, Token(3));
                let code = parts
                    .outcome
                    .bytes()
                    .expect("native outcome")
                    .err()
                    .expect("empty fixed slot");
                if code == Errno::CANCELED {
                    canceled += 1;
                } else {
                    assert_eq!(code, Errno::BAD_DESCRIPTOR);
                    failed += 1;
                }
                assert!(failed + canceled <= 3, "duplicate completion");
            }
            ring.publish();
        }
        assert_eq!((failed, canceled), (2, 1));
        assert_eq!(calls.get(), 3, "each option consumed once");
        assert!(ring.try_close().is_ok());
    })
    .join()
    .expect("split submission thread");
}
