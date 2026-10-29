pub mod common;

use std::{cell::Cell, net::SocketAddr, num};

use dope_sys::{
    Errno,
    io::reactor::complete::{
        self, Flags as _, Outcome as _, Packet as _, Value as _,
        message::{Cut, Error, Output as _, Region},
        packet::Invalid,
    },
    probe::{
        self,
        receive::{Corrupt, Failure, Receive as _},
    },
    ring::Source,
};
use dope_uring::Uring;
use o3::base::num::bounded;

use crate::common::{
    cleanup::Host,
    message::{PREFIX, prefix, try_buffer},
    packets::{Echo, Lease, flags, outcome, parts, rejected, token},
};

type Entry = <Uring as Source>::Entry<()>;
type Flags = <Entry as complete::Entry>::Flags;
type Output<'a, const NAME: usize, const CONTROL: usize> =
    <Uring as Source>::Message<'a, NAME, CONTROL>;
type Payload = <Uring as probe::Source>::Payload;

#[test]
fn control_word() {
    use dope_sys::table::token::{self, Codec as _, Context as _, frame::Decode as _};
    use dope_uring::Uring;
    use token::{Decoded, frame};

    type Token = <Uring as token::Source>::Token<Host>;
    type Frame = <Token as token::Codec>::Frame;

    for raw in [0, 1, 255, 1 << 56, 248 << 56, 250 << 56, 254 << 56] {
        assert_eq!(Frame::from_raw(raw).map(frame::Decode::control), Err(raw));
    }
    let key = num::NonZero::<u64>::MIN;
    assert!(matches!(Token::flight(key).decode(), Decoded::Flight(found) if found == key));
    assert!(
        matches!(Token::cancel_key(key).decode(), Decoded::CancelFlight(found) if found == key)
    );
    assert!(
        matches!(Token::shutdown().decode(), Decoded::Framework(control)
        if control.control() == Host::SHUTDOWN
            && matches!(control.decode(), frame::Control::Shutdown))
    );
    assert!(
        matches!(Token::prepare().decode(), Decoded::Framework(control)
        if matches!(control.decode(), frame::Control::ClosePrep))
    );
}

#[test]
fn retains_buffers_and_flow() {
    for buffer in 0..=u16::MAX {
        for bits in 0..8 {
            let flags = flags((u32::from(buffer) << 16) | bits);
            match flags.decode() {
                Ok(meta) => {
                    assert!(bits & 1 != 0 || buffer == 0);
                    assert!(Flags::from(meta) == flags);
                }
                Err(invalid) => {
                    assert!(bits & 1 == 0 && buffer != 0);
                    assert!(Flags::from(invalid) == flags);
                }
            }
        }
    }
}

#[test]
fn retains_unknown_bits() {
    for bit in 3..16 {
        for known in [0, 1, 2, 3, 4, 5, 6, 7, 0xffff_0007] {
            let flags = flags(known | (1 << bit));
            let decoded = flags.decode();
            assert!(decoded.is_err());
            if let Err(invalid) = decoded {
                assert!(Flags::from(invalid) == flags);
            }
        }
    }
}

#[test]
fn rejects_control_cut() {
    let mut storage = try_buffer::<4, 4>(8).expect("message buffer");
    prefix(
        storage.first_chunk_mut().expect("message prefix"),
        0,
        5,
        0,
        0,
    );
    assert!(matches!(
        Output::<4, 4>::parse(&storage),
        Err(Error::MissingTruncation {
            region: Region::Control,
            reported,
            available,
        }) if bounded::U64::<0, { u64::MAX }>::from(reported)
            == bounded::U64::clamp(5)
            && bounded::U64::<0, { u64::MAX }>::from(available)
                == bounded::U64::clamp(4)
    ));
}

#[test]
fn binds_payload_cut() {
    let mut storage = try_buffer::<4, 4>(3).expect("message buffer");
    prefix(
        storage.first_chunk_mut().expect("message prefix"),
        0,
        0,
        9,
        libc::MSG_TRUNC as u32,
    );
    let parsed = Output::<4, 4>::parse(&storage);

    let output = parsed.expect("flag proves payload truncation");
    assert_eq!(output.payload().len(), 3);
    assert!(output.payload_cut() == Cut::Truncated);
}

#[test]
fn verifies_probe_payload() {
    assert_eq!(Payload::bytes(b"a", b"a"), Ok(()));
    assert_eq!(Payload::bytes(b"a", b"b"), Err(Corrupt));

    let mut storage = try_buffer::<4, 0>(1).expect("message buffer");
    prefix(
        storage.first_chunk_mut().expect("message prefix"),
        4,
        0,
        1,
        0,
    );
    storage[PREFIX..PREFIX + 4].copy_from_slice(b"peer");
    storage[PREFIX + 4] = b'a';
    let local = |name: &[u8]| {
        assert_eq!(name, b"peer");
        Ok::<_, u8>(SocketAddr::from(([127, 0, 0, 1], 1)))
    };
    assert!(Payload::message::<4, _>(&storage, b"a", local).is_ok());
    assert!(matches!(
        Payload::message::<4, _>(&storage, b"b", local),
        Err(Failure::Payload(Corrupt))
    ));
    assert!(matches!(
        Payload::message::<4, _>(&storage, b"a", |_| {
            Ok::<_, u8>(SocketAddr::from(([192, 0, 2, 1], 1)))
        }),
        Err(Failure::Payload(Corrupt))
    ));
    assert!(matches!(
        Payload::message::<4, _>(&storage, b"a", |_| Err(7_u8)),
        Err(Failure::Address(7))
    ));

    prefix(
        storage.first_chunk_mut().expect("message prefix"),
        4,
        0,
        2,
        libc::MSG_TRUNC as u32,
    );
    let calls = Cell::new(0);
    let reject = |_: &[u8]| {
        calls.set(calls.get() + 1);
        Err::<SocketAddr, _>(7_u8)
    };
    assert!(matches!(
        Payload::message::<4, u8>(&storage, b"a", &reject),
        Err(Failure::Truncated)
    ));
    assert!(matches!(
        Payload::message::<4, u8>(&storage[..PREFIX], b"a", &reject),
        Err(Failure::Frame(Error::Short { .. }))
    ));
    assert_eq!(calls.get(), 0);
}

#[test]
fn rejects_cancel_overflow() {
    use dope_sys::table::token::{Codec as _, Decoded};

    let token = token((249_u64 << 56) | (1_u64 << 24));
    assert!(matches!(token.decode(), Decoded::Invalid));
}

#[test]
fn retire_keeps_authority() {
    for raw in [0, -libc::EIO, i32::MIN] {
        for flags in [0, 1, 2, 4, 8, 1 << 16, 1 | (1 << 16)] {
            for selected in [false, true] {
                let complete = Cell::new(0);
                let retry = Cell::new(0);
                let dropped = Cell::new(0);
                {
                    let mut parts = parts(flags);
                    parts.outcome = outcome(raw);
                    let buffer = selected.then(|| Lease(&dropped));
                    let packet = <Uring as Source>::Packet::new(parts, buffer);
                    assert_eq!(packet.parts(), parts);
                    assert_eq!(packet.flow() == complete::flags::Flow::More, flags & 2 != 0);
                    let settled = packet.try_retire(Echo(&complete, &retry));
                    let terminal = raw == 0 && flags == 0 && !selected;
                    assert_eq!(complete.get(), usize::from(terminal));
                    assert_eq!(retry.get(), usize::from(!terminal));
                    assert_eq!(settled.is_ok(), terminal);
                    if let Err(rejected) = settled {
                        assert!(std::ptr::eq(rejected, &packet));
                    }
                    assert_eq!(dropped.get(), 0);
                }
                assert_eq!(dropped.get(), usize::from(selected));
            }
        }
    }
}

#[test]
fn parts_retain_owner() {
    let closed = Cell::new(0);
    {
        let expected = parts(0);
        let packet = <Uring as Source>::Packet::new(expected, Some(Lease(&closed)));
        assert!(!packet.is_plain());
        let (returned, buffer) = packet.into_parts();
        assert!(returned == expected);
        assert!(buffer.is_some());
        assert_eq!(closed.get(), 0);
    }
    assert_eq!(closed.get(), 1);
}

#[test]
fn keeps_first_fault() {
    type Packet<B> = <Uring as Source>::Packet<u64, B>;
    let (packet, cause) = Packet::<Lease<'_>>::try_new(parts(8), Err(13_u8))
        .err()
        .expect("buffer")
        .into_parts();
    assert!(matches!(cause, Invalid::Buffer(13)));
    assert!(packet.parts().flags == parts(8).flags);
    assert!(packet.buffer().is_none());
    let closed = Cell::new(0);
    {
        let (packet, cause) = Packet::try_new(parts(8), Ok::<_, u8>(Some(Lease(&closed))))
            .err()
            .expect("flags")
            .into_parts();
        assert!(matches!(cause, Invalid::Flags));
        assert!(packet.parts().flags == parts(8).flags);
        assert!(packet.buffer().is_some());
        assert_eq!(closed.get(), 0);
    }
    assert_eq!(closed.get(), 1);
}

#[test]
fn keeps_exact_results() {
    for (raw, error) in [
        (i32::MIN, false),
        (-i32::MAX, true),
        (-1, true),
        (0, false),
        (1, false),
        (i32::MAX, false),
    ] {
        let value = outcome(raw);
        assert_eq!(value.is_error(), error);
        match value.bytes() {
            Err(returned) => {
                assert_eq!(raw, i32::MIN);
                assert_eq!(rejected(returned), raw);
            }
            Ok(Err(code)) => assert_eq!(code.code().get(), -raw),
            Ok(Ok(bytes)) => {
                let bytes: bounded::U32<0, { i32::MAX as u32 }> = bytes.into();
                assert_eq!(bytes.get(), raw as u32);
            }
        }
        match value.unit() {
            Err(returned) => {
                assert!(raw == i32::MIN || raw > 0);
                assert_eq!(rejected(returned), raw);
            }
            Ok(Err(code)) => assert_eq!(code.code().get(), -raw),
            Ok(Ok(())) => assert_eq!(raw, 0),
        }
        if let complete::Decode::Value(value) = value.decode() {
            assert_eq!(value.is_zero(), raw == 0);
        }
    }
}

#[test]
fn keeps_cancel_classes() {
    for (raw, canceled, aborted) in [
        (-libc::ENOENT, true, false),
        (-libc::EALREADY, true, false),
        (-libc::ECANCELED, true, true),
        (0, false, false),
        (1, false, false),
        (-libc::EIO, false, false),
        (i32::MIN, false, false),
    ] {
        let value = outcome(raw);
        for (result, expected) in [(value.canceled(), canceled), (value.aborted(), aborted)] {
            assert_eq!(result.is_ok(), expected);
            if let Err(returned) = result {
                assert_eq!(rejected(returned), raw);
            }
        }
    }
    assert!(matches!(
        outcome(-libc::ECANCELED).unit(),
        Ok(Err(Errno::CANCELED))
    ));
}

#[test]
fn keeps_shutdown_mask() {
    for bits in [
        libc::POLLIN,
        libc::POLLIN | libc::POLLHUP,
        libc::POLLIN | libc::POLLRDHUP,
        libc::POLLIN | libc::POLLHUP | libc::POLLRDHUP,
    ] {
        let value = outcome(i32::from(bits));
        assert!(value.shutdown().is_ok());
        assert!(value.stopped().is_ok());
    }
    for raw in [
        0,
        i32::from(libc::POLLHUP),
        i32::from(libc::POLLIN | libc::POLLERR),
        i32::from(libc::POLLOUT),
        i32::MAX,
        i32::MIN,
        -libc::EIO,
        -libc::ECANCELED,
    ] {
        let value = outcome(raw);
        assert!(value.shutdown().is_err());
        if let Err(returned) = value.shutdown() {
            assert_eq!(rejected(returned), raw);
        }
        assert_eq!(value.stopped().is_ok(), raw == -libc::ECANCELED);
        if let Err(returned) = value.stopped() {
            assert_eq!(rejected(returned), raw);
        }
    }
}
