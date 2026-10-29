pub mod common;

use std::{array, time::Duration};

use dope_sys::{
    Errno,
    io::{
        Close as _,
        reactor::{
            Full,
            complete::{
                Decode, Drain as _, Entry as _, Head as _, Inspect as _, Outcome as _, Value as _,
                flags, flags::Meta as _,
            },
            submit::{self, Control as _, Poll as _, Queue as _, Ready as _},
        },
    },
    ring::{
        Access as _, Complete as _, Owner as _, Source,
        enter::{self, Enter},
        inspect::Inspect as _,
        prepare::Prepare as _,
        register::{Registered as _, Registry as _},
    },
};
use dope_uring::Uring;
use o3::base::num::bounded;

use crate::common::ring::{BATCH_WIDTH, BATCHES, CAPACITY, Inspect as _, Ring as _, Token};

type Poll<'a> = <Uring as submit::Control>::Poll<'a, Token>;
type Timeout<'a> = <<Uring as Source>::Enter<'a, Token> as Enter>::Timeout;

#[test]
fn cancel_all_accepts_count() {
    use std::os::{fd::AsFd as _, unix::net::UnixStream};

    const TOKEN: Token = Token(0xfeed_cafe);

    let mut ring = Uring::try_ring::<2, 4>(false).expect("ring fixture");
    let (reader, _writer) = UnixStream::pair().expect("unix stream pair");
    let poll = Poll::new(reader.as_fd(), TOKEN);
    {
        let queue = ring.sq();
        // SAFETY: `reader` remains live until the cancelled poll CQE is consumed
        // and the ring is closed below.
        let vacancy = queue.try_one().expect("poll submission capacity");
        let prepared = unsafe { submit::raw::Vacancy::borrow(vacancy, poll.entry()) };
        prepared.push();
    }
    assert!(ring.enter().try_submit().is_ok(), "poll submission");
    assert!(
        ring.registry().cancel_all().is_ok(),
        "synchronous cancellation"
    );
    let timeout =
        Timeout::try_from(Duration::from_secs(1)).expect("cancel completion wait duration");
    assert!(
        ring.enter().try_wait(Some(timeout)).is_ok(),
        "cancel completion wait"
    );
    let mut queue = ring.completion();
    queue.acquire();
    let entry = queue.pop().expect("cancelled poll produced no completion");
    let parts = entry.into_parts();
    {
        use dope_sys::{
            io::reactor::complete::{Observe, Record},
            probe::{self, Outcome as _},
        };
        use dope_uring::Uring;

        type Checked = <Uring as probe::Source>::Outcome<Token, u8>;
        let done = Checked::new(parts, 7).expect("valid cancellation evidence");
        assert_eq!(done.probe(), 7);
        assert!(done.canceled().is_ok());
        let rejected = Checked::new(parts, 7)
            .expect("valid cancellation flags")
            .zero()
            .expect_err("cancellation is not zero success");
        let (probe, cause) = rejected.into_parts();
        assert_eq!(probe, 7);
        let expected =
            <Uring as Observe<Token>>::Observation::new(parts.token, parts.outcome, parts.flags);
        assert!(matches!(cause, probe::Check::Rejected(observed) if observed == expected));
    }
    parts
        .try_inspect(|token, value, meta| {
            assert!(token == TOKEN);
            assert!(matches!(value, Decode::Error(code) if code == Errno::CANCELED));
            assert!(meta.flow() == flags::Flow::Last);
        })
        .expect("completion flags");
    queue.publish();
    assert!(ring.try_close().is_ok(), "ring teardown failed");
}

#[test]
fn multishot_keeps_tail() {
    use std::{
        io::{Read as _, Write as _},
        os::{fd::AsFd as _, unix::net::UnixStream},
    };

    const TOKEN: Token = Token(0x51a7_0001);
    const SHOTS: u8 = 64;

    let mut ring = Uring::try_ring::<4, 8>(true).expect("ring fixture");
    let (mut reader, mut writer) = UnixStream::pair().expect("unix stream pair");
    let poll = Poll::multi(reader.as_fd(), TOKEN);
    {
        let queue = ring.sq();
        // SAFETY: `reader` remains live through every multishot CQE and ring
        // teardown in this test.
        let vacancy = queue.try_one().expect("multishot poll capacity");
        let prepared = unsafe { submit::raw::Vacancy::borrow(vacancy, poll.entry()) };
        prepared.push();
    }

    for shot in 0..SHOTS {
        assert!(writer.write_all(&[shot]).is_ok(), "write poll stimulus");
        let timeout =
            Timeout::try_from(Duration::from_secs(1)).expect("multishot completion wait duration");
        assert!(
            ring.enter().try_wait(Some(timeout)).is_ok(),
            "multishot completion wait"
        );
        let mut queue = ring.completion();
        queue.acquire();
        let entry = queue.pop().expect("multishot poll produced no completion");
        entry
            .into_parts()
            .try_inspect(|token, value, meta| {
                assert!(token == TOKEN);
                let value = match value {
                    Decode::Value(value) => Some(value),
                    _ => None,
                }
                .expect("poll completion failed");
                let value: bounded::U32<0, { i32::MAX as u32 }> = value.into();
                assert!(value.get() & u32::from(libc::POLLIN.unsigned_abs()) != 0);
                assert!(meta.flow() == flags::Flow::More);
            })
            .expect("completion flags");
        queue.publish();

        let mut observed = [0];
        assert!(
            reader.read_exact(&mut observed).is_ok(),
            "consume poll stimulus"
        );
        assert_eq!(observed, [shot]);
    }

    let cancel = Uring::cancel(TOKEN, TOKEN);
    {
        let queue = ring.sq();
        let vacancy = queue.try_one().expect("cancel submission capacity");
        let prepared = submit::raw::Vacancy::prepare(vacancy, &cancel);
        prepared.push();
    }
    let timeout =
        Timeout::try_from(Duration::from_secs(1)).expect("multishot cancellation wait duration");
    assert!(
        ring.enter().try_wait(Some(timeout)).is_ok(),
        "multishot cancellation wait"
    );
    let mut queue = ring.completion();
    queue.acquire();
    let entry = queue
        .pop()
        .expect("multishot poll produced no terminal completion");
    entry
        .into_parts()
        .try_inspect(|token, value, meta| {
            assert!(token == TOKEN);
            assert!(matches!(value, Decode::Error(code) if code == Errno::CANCELED));
            assert!(meta.flow() == flags::Flow::Last);
        })
        .expect("completion flags");
    queue.publish();
    assert!(ring.try_close().is_ok(), "ring teardown failed");
}

#[test]
fn batches_wrap() {
    let ring = Uring::try_ring::<{ CAPACITY }, { CAPACITY * 2 }>(true).expect("ring fixture");
    let files = bounded::U32::<1, { u32::MAX }>::exact::<16>();
    let range = bounded::U32::<1, { u32::MAX }>::exact::<8>();
    let mut ring = ring.prepare(files, Some(range)).expect("ring admission");

    assert!(ring.inspect().is_ok());

    let mut registry = ring.registry();
    let canceled = registry.cancel(Token(u64::MAX));
    assert!(
        matches!(canceled, Ok(count) if count.get() == 0)
            || matches!(canceled, Err(errno) if errno.is_code(Errno::NOT_FOUND))
    );
    let mut ring = ring.try_register().expect("ring registration failed");
    assert_eq!(ring.refresh().get(), 0);

    for batch in 0..BATCHES {
        let base = batch * u64::from(BATCH_WIDTH);
        let mut queue = ring.sq();
        queue.refresh();
        assert!(queue.is_empty());
        let queue = ring.sq();
        let entries: [_; BATCH_WIDTH as usize] =
            array::from_fn(|index| Uring::nop(Token(base + index as u64)));
        let prepared = queue.prepare(&entries).expect("batch capacity");
        prepared.push();
        let overflow: [_; (CAPACITY - BATCH_WIDTH + 1) as usize] =
            array::from_fn(|index| Uring::nop(Token(u64::MAX - index as u64)));
        let queue = ring.sq();
        assert!(matches!(queue.prepare(&overflow), Err(Full)));

        let mut seen = [false; BATCH_WIDTH as usize];
        let mut done = 0;
        while done != BATCH_WIDTH {
            let timeout =
                Timeout::try_from(Duration::from_secs(1)).expect("completion wait duration");
            assert!(
                ring.enter().try_wait(Some(timeout)).is_ok(),
                "io_uring enter"
            );
            ring.acquire();
            while let Some(entry) = ring.pop() {
                entry
                    .into_parts()
                    .try_inspect(|token, value, meta| {
                        assert!(matches!(value, Decode::Value(value) if value.is_zero()));
                        assert!(meta.flow() == flags::Flow::Last);
                        assert!(meta.buffer().is_none());
                        let index = (token.0.checked_sub(base))
                            .expect("completion user_data precedes this batch");
                        assert!(index < u64::from(BATCH_WIDTH));
                        assert!(!seen[index as usize]);
                        seen[index as usize] = true;
                        done += 1;
                    })
                    .expect("completion flags");
            }
            ring.refresh();
        }
    }
    assert!(ring.try_close().is_ok(), "ring teardown failed");
}

#[test]
fn commit_tracks_tail() {
    const TOKEN: Token = Token(0xc011_1701);

    let mut ring = Uring::try_ring::<2, 4>(false).expect("ring fixture");
    assert!(matches!(
        ring.enter().try_commit(),
        Ok(enter::Progress::Idle)
    ));

    let nop = Uring::nop(TOKEN);
    {
        let queue = ring.sq();
        let vacancy = queue.try_one().expect("nop submission capacity");
        let prepared = submit::raw::Vacancy::prepare(vacancy, &nop);
        prepared.push();
    }
    assert!(matches!(
        ring.enter().try_commit(),
        Ok(enter::Progress::Entered)
    ));

    let timeout =
        Timeout::try_from(Duration::from_secs(1)).expect("committed completion wait duration");
    assert!(
        ring.enter().try_wait(Some(timeout)).is_ok(),
        "committed completion wait"
    );
    let mut queue = ring.completion();
    queue.acquire();
    let entry = queue.pop().expect("committed Nop produced no completion");
    entry
        .into_parts()
        .try_inspect(|token, value, meta| {
            assert!(token == TOKEN);
            assert!(matches!(value, Decode::Value(value) if value.is_zero()));
            assert!(meta.flow() == flags::Flow::Last);
        })
        .expect("completion flags");
    queue.publish();

    assert!(matches!(
        ring.enter().try_commit(),
        Ok(enter::Progress::Idle)
    ));
    assert!(ring.try_close().is_ok(), "ring teardown failed");
}

#[test]
fn large_backing_reopens() {
    for sequence in 0..8 {
        let mut ring = Uring::try_ring::<8192, 65536>(false).expect("large ring backing");
        let entry = Uring::nop(Token(sequence));
        let vacancy = ring.sq().try_one().expect("large ring submission");
        submit::raw::Vacancy::prepare(vacancy, &entry).push();
        let timeout = Timeout::try_from(Duration::from_secs(1)).expect("completion duration");
        ring.enter()
            .try_wait(Some(timeout))
            .expect("large ring completion");
        let mut queue = ring.completion();
        queue.acquire();
        queue
            .pop()
            .expect("large ring returned the submitted token")
            .into_parts()
            .try_inspect(|token, value, meta| {
                assert_eq!(token.0, sequence);
                assert!(matches!(value, Decode::Value(value) if value.is_zero()));
                assert!(meta.flow() == flags::Flow::Last);
            })
            .expect("completion flags");
        queue.publish();
        assert!(ring.try_close().is_ok(), "large backing closes");
    }
}

#[test]
fn inspection_retains_head() {
    let mut ring = Uring::try_ring::<1, 2>(false).expect("ring fixture");
    for token in 0..16 {
        let entry = Uring::nop(Token(token));
        let vacancy = ring
            .sq()
            .try_acquire()
            .expect("acquired submission capacity");
        submit::raw::Vacancy::prepare(vacancy, &entry).push();
        assert!(matches!(ring.sq().try_acquire(), Err(Full)));
        let timeout = Timeout::try_from(Duration::from_secs(1)).expect("completion duration");
        ring.enter()
            .try_wait(Some(timeout))
            .expect("submitted completion");
        let mut queue = ring.completion();
        assert_eq!(queue.len().get(), 0);
        assert_eq!(queue.available().get(), 1);
        {
            let head = queue.front().expect("published completion");
            let parts = head.parts();
            assert_eq!(parts.token.0, token);
            assert!(matches!(parts.outcome.decode(), Decode::Value(value) if value.is_zero()));
        }
        assert_eq!(queue.len().get(), 0);
        assert_eq!(queue.available().get(), 1);
        let head = queue.front().expect("inspection retained the completion");
        assert_eq!(head.parts().token.0, token);
        head.commit();
        assert_eq!(queue.available().get(), 0);
        assert!(queue.front().is_none());
    }
    assert!(ring.try_close().is_ok());
}
