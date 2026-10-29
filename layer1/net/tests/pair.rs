use dope_core::io::transfer;
use dope_net::link::egress::{
    self,
    data::{self, pair, raw::Payload as _},
    load::lease,
};
use dope_test::alloc::Tracker;
use o3::buffer::{
    self, pool,
    raw::Scatter,
    resident::{self, bound},
    segments::scatter,
    storage,
};

pub mod common;

use common::chain::Restore;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn traverses_suffixes() {
    let pool = pool::Pool::try_new(1, 16);
    let pool = (pool).expect("required fixture outcome");
    let mut prefix = (pool.cursor()).expect("prefix slot missing");
    let append = prefix.try_append(b"ab");
    let append = (append).expect("required fixture outcome");
    append.commit();
    let head_ptr = prefix.as_ref().as_ptr();
    let body = resident::Shared::from(&b"cdef"[..]);
    let body_ptr = body.as_ref().as_ptr();
    let ((), alloc) = Tracker::usage(|| {
        let pair = pair::Source::try_new(prefix, body);
        assert!(pair.is_ok(), "required fixture outcome");
        let Some(pair) = pair.ok() else { return };
        assert!(pair.geometry().wire() == transfer::NonZero::fixed::<6>());
        assert_eq!(pair.as_inner().charge().resident_bytes, 16);
        for offset in 0..6 {
            let len = transfer::Len::try_new(offset);
            let len = (len).expect("required fixture outcome");
            for cap in [1, 2, 3, 8] {
                let cursor = pair.as_inner().cursor(data::Offset::from_len(len));
                assert!(cursor.is_some());
                let Some(cursor) = cursor else { return };
                let mut chunks = cursor;
                let mut position = offset;
                while chunks.remaining() != 0 {
                    let before = position;
                    for chunk in chunks.take_bytes(cap) {
                        let bytes = chunk.bytes();
                        let end = position + bytes.len();
                        assert_eq!(bytes, &b"abcdef"[position..end]);
                        let expected = if position < 2 {
                            head_ptr.wrapping_add(position)
                        } else {
                            body_ptr.wrapping_add(position - 2)
                        };
                        assert_eq!(bytes.as_ptr(), expected);
                        position = end;
                    }
                    assert!(position > before);
                    assert!(position - before <= cap);
                    assert_eq!(chunks.remaining(), 6 - position);
                }
                assert_eq!(position, 6);
            }
        }
        assert!(
            pair.as_inner()
                .cursor(data::Offset::from_len(transfer::Len::fixed::<6>()))
                .is_none()
        );
        assert!(
            pair.as_inner()
                .cursor(data::Offset::from_len(transfer::Len::fixed::<7>()))
                .is_none()
        );
    });
    assert_eq!(alloc, (0, 0));
    assert_eq!(pool.available(), 1);
}

#[test]
fn retains_empty_owners() {
    let pool = pool::Pool::try_new(1, 16);
    let pool = (pool).expect("required fixture outcome");
    {
        let prefix = (pool.cursor()).expect("prefix slot missing");
        let result = pair::Source::try_new(prefix, storage::Shared::new());
        assert!(result.is_err());
        let Err(rejected) = result else { return };
        let (mut prefix, body, cause) = rejected.into_parts();
        assert!(prefix.as_ref().is_empty());
        assert!(body.as_ref().is_empty());
        assert!(matches!(cause, pair::Error::Empty(_)));
        let append = prefix.try_append(b"recovered");
        let append = (append).expect("required fixture outcome");
        append.commit();
        let retry = pair::Source::try_new(prefix, body);
        assert!(retry.is_ok(), "required fixture outcome");
        let Some(retry) = retry.ok() else { return };
        assert!(retry.geometry().wire() == transfer::NonZero::fixed::<9>());
    }
    assert_eq!(pool.available(), 1);
}

#[test]
fn remeasures_empty_owners() {
    let pool = pool::Pool::try_new(1, 16).expect("prefix pool");
    let mut body =
        resident::Shared::from(storage::Shared::try_copy(b"body").expect("body storage"));
    buffer::raw::Slice::try_with(&mut body, 0..0, buffer::raw::Store::restrict)
        .expect("empty body range");
    for (head, body, logical, owners) in [
        (&b""[..], resident::Shared::from(&b"body"[..]), 0, 1),
        (&b"head"[..], body, 4, 2),
    ] {
        let mut prefix = pool.cursor().expect("prefix slot");
        prefix.try_append(head).expect("prefix bytes").commit();
        let pair = pair::Source::try_new(prefix, body).ok().expect("pair");
        let charge = pair.as_inner().charge();
        assert_eq!(charge.logical_bytes, logical);
        assert_eq!(charge.owners, owners);
        assert_eq!(egress::Charge::of(pair.as_inner()), pair.charge());
        let pair = egress::NonEmpty::try_new(pair.into_inner()).expect("remeasured pair");
        assert!(pair.try_bound::<bound::AtMost<0>>().is_err());
    }
}

#[test]
fn restores_cancelled_body() {
    let pool = pool::Pool::try_new(2, 16);
    let pool = (pool).expect("required fixture outcome");
    let prefix = (pool.cursor()).expect("prefix slot missing");
    let body = pool.try_copy(b"body");
    let body = body
        .expect("fixture payload rejected")
        .expect("fixture payload absent");
    let body = body.into_shared();
    let pointer = body.as_ref().as_ptr();
    let mut restored = Vec::with_capacity(1);
    let mut commits = Some(0);
    let ((), alloc) = Tracker::usage(|| {
        let pair = pair::Source::try_new(prefix, body);
        assert!(pair.is_ok(), "required fixture outcome");
        let Some(pair) = pair.ok() else { return };
        let leased = pair.lease(Restore {
            body: &mut restored,
            commits: &mut commits,
        });
        assert!(leased.geometry().wire() == transfer::NonZero::fixed::<4>());
        assert_eq!(pool.available(), 0);
    });
    assert_eq!(alloc, (0, 0));
    assert_eq!(commits, Some(0));
    assert_eq!(pool.available(), 1);
    assert_eq!(restored.len(), 1);
    let body = restored.pop().expect("returned body");
    assert_eq!(body.as_ref().as_ptr(), pointer);
    assert_eq!(body.as_ref(), b"body");
    assert_eq!(
        size_of::<lease::Lease<Restore<'_>>>(),
        size_of::<Restore<'_>>() + size_of::<pair::Source<storage::Shared>>()
    );
}

#[test]
fn resumes_split_edges() {
    let pool = pool::Pool::try_new(1, 16).expect("prefix pool");
    let bytes = b"abcdef";
    for split in [0, 2, 6] {
        let mut prefix = pool.cursor().expect("prefix slot");
        prefix
            .try_append(&bytes[..split])
            .expect("prefix bytes")
            .commit();
        let body = resident::Shared::from(&bytes[split..]);
        let pair = pair::Source::try_new(prefix, body).ok().expect("pair");
        let payload = pair.as_inner();
        let mut position = scatter::raw::Position::new();
        for start in 0..bytes.len() {
            let offset = transfer::Len::try_new(start).expect("offset");
            // SAFETY: the position traverses only this unchanged pair, and every
            // offset lies strictly inside its six-byte concatenation.
            let mut cursor =
                unsafe { payload.resume(&mut position, data::Offset::from_len(offset)) };
            let mut used = start;
            for chunk in cursor.take_bytes(bytes.len()) {
                let end = used + chunk.bytes().len();
                assert_eq!(chunk.bytes(), &bytes[used..end]);
                used = end;
            }
            assert_eq!(used, bytes.len());
        }
    }
}
