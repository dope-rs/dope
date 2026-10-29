use dope_core::io::{
    transfer,
    transfer::{Len, progress},
};
use o3::buffer::{
    self,
    raw::Scatter as _,
    segments::scatter::{self, Source as _},
};

use crate::common::file::Message;

pub mod common;

#[test]
fn exact_rejects_overflow() {
    let observed = Len::try_new(transfer::MAX_BYTES + 1);

    assert!(matches!(observed, Err(cause) if cause.actual() == transfer::MAX_BYTES + 1));
}

#[test]
fn positive_sum_bounds() {
    let one = transfer::NonZero::fixed::<1>();
    let max = transfer::NonZero::fixed_size::<{ transfer::MAX_BYTES }>();

    assert_eq!(one.checked_add(Len::ZERO), Ok(one));
    assert_eq!(max.checked_add(Len::ZERO), Ok(max));
    assert_eq!(
        one.checked_add(Len::fixed::<{ transfer::MAX_BYTES - 1 }>()),
        Ok(max)
    );
}

#[test]
fn positive_sum_overflow() {
    let max = transfer::NonZero::fixed_size::<{ transfer::MAX_BYTES }>();

    assert!(matches!(
        max.checked_add(Len::fixed::<1>()),
        Err(cause) if cause.actual() == transfer::MAX_BYTES + 1
    ));
    assert!(matches!(
        max.checked_add(Len::MAX),
        Err(cause) if cause.actual() == 2 * transfer::MAX_BYTES
    ));
}

#[test]
fn keeps_positive_remainder() {
    let partial = Len::fixed::<2>().checked_sub(Len::fixed::<1>());

    assert!(matches!(partial, Some(rest) if rest.into_nonzero().get() == 1));
    assert!(Len::fixed::<1>().checked_sub(Len::fixed::<1>()).is_none());
    assert!(Len::fixed::<1>().checked_sub(Len::fixed::<2>()).is_none());
}

#[test]
fn progress_keeps_bounds() {
    let mut state = progress::State::new(Len::fixed::<7>());

    assert!(state.left() == Len::fixed::<7>());
    let step = state.reserve(Len::fixed::<3>());
    assert!(step.amount() == Len::fixed::<3>());
    step.commit();
    assert!(state.left() == Len::fixed::<4>());
    assert!(state.len() == Len::fixed::<3>());
}

#[test]
fn reports_excess() {
    let state = progress::State::new(Len::fixed::<7>());
    let cause = (state.try_extend(Len::fixed::<8>()))
        .err()
        .expect("extension beyond remaining capacity succeeded");

    assert!(cause.requested() == Len::fixed::<8>());
    assert!(cause.available() == Len::fixed::<7>());
}

#[test]
fn scatter_commits_prefix() {
    let source = b"abcdefgh";
    let mut cursor = source.as_slice();
    let mut budget = progress::State::new(Len::fixed::<3>());
    let part = budget.scatter(&mut cursor).expect("bounded prefix");
    assert_eq!(part.bytes(), b"abc");
    assert_eq!(part.bytes().as_ptr(), source.as_ptr());
    assert!(budget.len() == Len::fixed::<3>());
    assert!(budget.is_empty());
    assert_eq!(cursor, b"defgh");
    assert!(budget.scatter(&mut cursor).is_none());
    assert_eq!(cursor, b"defgh");

    let mut retry = progress::State::new(Len::fixed::<7>());
    let part = retry.scatter(&mut cursor).expect("remaining prefix");
    assert_eq!(part.bytes(), b"defgh");
    assert_eq!(part.bytes().as_ptr(), source.as_ptr().wrapping_add(3));
    assert!(retry.len() == Len::fixed::<5>());
    assert!(retry.left() == Len::fixed::<2>());
    assert!(retry.scatter(&mut cursor).is_none());
    assert!(retry.left() == Len::fixed::<2>());
}

#[test]
fn cursor_keeps_owner() {
    let mut input = Message::default();
    let mut chunks = {
        let graph = input.view();
        graph.chunks()
    };
    let first = scatter::Take::new(&mut chunks, 3)
        .map(|part| part.bytes())
        .collect::<Vec<_>>();
    assert_eq!(first, [b"ab".as_slice(), b"c".as_slice()]);
    assert_eq!(chunks.remaining(), 3);
    let last = scatter::Take::new(&mut chunks, usize::MAX)
        .next()
        .expect("remaining chunk");
    assert_eq!(last.bytes(), b"def");
    assert_eq!(chunks.remaining(), 0);
}

#[test]
fn offset_keeps_owner() {
    let mut input = Message::default();
    assert!(buffer::raw::Chunks::cursor(&input.view(), 7).is_none());
    for offset in 0..=6 {
        let cursor = {
            let graph = input.view();
            buffer::raw::Chunks::cursor(&graph, offset).expect("bounded offset")
        };
        let mut chunks = cursor;
        let bytes = scatter::Take::new(&mut chunks, usize::MAX)
            .flat_map(|part| part.bytes())
            .copied()
            .collect::<Vec<_>>();
        assert_eq!(bytes, b"abcdef"[offset..]);
    }
}
