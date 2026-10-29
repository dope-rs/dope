pub mod common;

use std::mem;

use dope_sys::memory::buffer::{self, Index as _, provided};
use dope_uring::Uring;

type Buffer = <Uring as provided::Source>::Index;

#[test]
fn provided_owners_wrap() {
    use std::slice;

    use o3::{base::num::bounded, buffer::bytes::bounded::MAX};

    use crate::common::receive::Layout;
    type Len = bounded::U32<0, { MAX as u32 }>;
    use dope_sys::{
        io::Close as _,
        memory::buffer::{
            provided::{self, Context as _, Producer as _},
            raw::{Context as _, Pending as _, Token as _},
        },
    };
    use dope_uring::Uring;

    type Context = <Uring as provided::Source>::Context<Layout>;
    type Producer = <Uring as provided::Source>::Producer<Layout>;

    let context = Context::new(Layout::fixed::<2, 16>()).expect("populated payload");
    // SAFETY: this exclusive mapping contains both sixteen-byte slots.
    unsafe { context.as_ptr().cast_mut().write_bytes(7, 32) };
    let mut producer = Producer::new(&context).expect("ring mapping");
    let (entries, count) = producer.registration();
    assert_eq!(count.get(), 2);
    let tail = entries.cast::<u8>().as_ptr().wrapping_add(14).cast::<u16>();
    // SAFETY: registration retains two initialized ABI entries until close.
    assert_eq!(unsafe { tail.read() }, 2);

    let zero = Buffer::ZERO;
    let mut invalid = zero;
    invalid.advance();
    invalid.advance();
    // SAFETY: this emulates a CQE; rejection must not create an owner.
    assert!(unsafe { buffer::raw::Select::select(&producer, invalid) }.is_err());
    // SAFETY: this emulates the first CQE transferring slot zero once.
    let pending = unsafe { buffer::raw::Select::select(&producer, zero) }.expect("selected slot");
    let (pending, actual, limit) = pending
        .try_complete(Len::exact::<17>())
        .expect_err("oversized completion");
    assert_eq!(actual.get(), 17);
    assert_eq!(limit.get(), 16);
    assert!(pending.index() == zero);
    let done = pending
        .try_complete(Len::exact::<4>())
        .expect("live prefix");
    // SAFETY: this token was selected from the producer created for context;
    // its four-byte completion is initialized and remains uniquely retained.
    assert_eq!(unsafe { context.bytes(&done) }, &[7; 4]);
    // SAFETY: the completed token retains this producer's slot; its borrow has ended.
    unsafe { buffer::raw::Release::release(&mut producer, done) };
    // SAFETY: deferred writes do not publish the initialized first tail field.
    assert_eq!(unsafe { tail.read() }, 2);
    producer.flush();
    // SAFETY: the producer retains this same live first entry.
    assert_eq!(unsafe { tail.read() }, 3);

    let mut next = zero;
    next.advance();
    for turn in 1..=u16::MAX {
        let index = if turn & 1 == 0 { zero } else { next };
        // SAFETY: each emulated CQE reacquires the slot after its prior return.
        let pending =
            unsafe { buffer::raw::Select::select(&producer, index) }.expect("returned slot");
        // SAFETY: the emulated CQE transferred this producer's unique slot.
        unsafe { buffer::raw::Release::release(&mut producer, pending) };
        producer.flush();
    }
    // SAFETY: the complete u16 cycle retains and wraps the published tail.
    assert_eq!(unsafe { tail.read() }, 2);
    // SAFETY: both stable entries are initialized and retained by producer.
    let bytes = unsafe { slice::from_raw_parts(entries.cast::<u8>().as_ptr(), 32) };
    assert_eq!(&bytes[8..12], &16_u32.to_ne_bytes());
    assert_eq!(&bytes[24..28], &16_u32.to_ne_bytes());
    assert_eq!(&bytes[12..14], &0_u16.to_ne_bytes());
    assert_eq!(&bytes[28..30], &1_u16.to_ne_bytes());
    assert!(producer.try_close().is_ok());
    assert!(context.try_close().is_ok());
}

#[test]
fn canary_retains_scalar() {
    use o3::{base::num::bounded, buffer::bytes::bounded::MAX};

    use crate::common::receive::Layout;
    type Len = bounded::U32<0, { MAX as u32 }>;
    use dope_sys::{
        io::Close as _,
        memory::buffer::{
            Index as _,
            canary::{Failure, raw::Storage as _},
            provided,
        },
    };
    use dope_uring::Uring;

    type Canary = <Uring as provided::Source>::Canary<Layout>;
    type Index = <Uring as provided::Source>::Index;

    assert_eq!(mem::size_of::<Canary>(), 64);
    let mut first = Canary::new().expect("first canary");
    let mut second = Canary::new().expect("second canary");
    // SAFETY: neither owner has been registered or borrowed by the kernel.
    let address = unsafe { first.option() } as *const libc::c_int;
    mem::swap(&mut first, &mut second);
    // SAFETY: moving the owner retains its exclusive stable scalar mapping.
    assert_eq!(unsafe { second.option() } as *const libc::c_int, address);

    let mut invalid = Index::ZERO;
    invalid.advance();
    invalid.advance();
    // SAFETY: this emulates an invalid CQE and cannot transfer any valid slot.
    let result = unsafe { second.inspect(invalid, Len::exact::<0>(), |_| Ok::<_, u8>(())) };
    assert!(matches!(result, Err(Failure::Index(index)) if index == invalid));

    let capacity = Layout::CANARY.get() as usize;
    let overflow = Len::new((capacity + 1) as u32).expect("oversized length");
    // SAFETY: this emulates one unique slot-zero CQE; rejection returns the slot.
    let result = unsafe { second.inspect(Index::ZERO, overflow, |_| Ok::<_, u8>(())) };
    assert!(
        matches!(result, Err(Failure::Length { actual, capacity: limit })
        if actual == overflow && limit.get() as usize == capacity)
    );

    // SAFETY: the preceding rejection returned this slot before its next CQE.
    let result = unsafe {
        second.inspect(Index::ZERO, Len::exact::<4>(), |bytes| {
            assert_eq!(bytes, &1_i32.to_ne_bytes());
            Err::<(), _>(7_u8)
        })
    };
    assert!(matches!(result, Err(Failure::Inspect(7))));
    // SAFETY: an inspector error also returns the slot before its next CQE.
    assert!(unsafe { second.inspect(Index::ZERO, Len::exact::<4>(), |_| Ok::<_, u8>(())) }.is_ok());
    assert!(first.try_close().is_ok());
    assert!(second.try_close().is_ok());
}

#[test]
fn metadata_population() {
    use std::thread;

    use dope_sys::{
        io::{Close as _, Retry as _},
        memory::{
            Error,
            buffer::provided::{Context as _, Failure as _, Open as _, Producer as _},
        },
    };

    use crate::common::{
        cleanup::schedule::{self, Schedule},
        receive::Layout,
        ring::seccomp,
    };

    type Context = <Uring as provided::Source>::Context<Layout>;
    type Producer = <Uring as provided::Source>::Producer<Layout>;

    thread::spawn(|| {
        let context = Context::new(Layout::fixed::<2, 16>()).expect("payload population");
        seccomp::deny_populate().expect("thread-local population filter");
        let failures = Schedule::new(schedule::ERRORS)
            .try_run(|| {
                let error = Producer::new(&context).expect_err("metadata population failure");
                let (primary, cleanup) = error.close(context).into_parts();
                assert!(matches!(primary, Error::Populate { bytes, cause }
                    if bytes.get() == 32 && cause.code().get() == libc::ENOMEM));
                let state = cleanup.expect("metadata and context retained");
                let (owner, first) = state.recover().into_parts();
                assert!(matches!(first, Error::Unmap { cause, .. }
                    if cause.code().get() == libc::ENOMEM));
                let state = owner.try_close().expect_err("second unmap failure");
                let (owner, second) = state.recover().into_parts();
                assert!(matches!(second, Error::Unmap { cause, .. }
                    if cause.code().get() == libc::EAGAIN));
                assert!(owner.try_close().is_ok());
            })
            .expect("two metadata cleanup failures");
        assert_eq!(failures[0].address, failures[1].address);
        assert_eq!(failures[0].length, 32);
        assert_eq!(failures[0].length, failures[1].length);
    })
    .join()
    .expect("population cleanup remains on its owning thread");
}
