use super::*;
use crate::driver::token::{Epoch, SlotIndex};

fn cqe(op: u8, result: i32, flags: u32) -> Cqe {
    Cqe {
        user_data: Token::new(7, SlotIndex::new(3), Epoch::INITIAL)
            .with_kind(op)
            .raw(),
        result,
        flags,
    }
}

fn kind(op: u8, result: i32, flags: u32) -> EventKind {
    Event::decode(cqe(op, result, flags)).unwrap().into_kind()
}

#[test]
fn decode_all_completion_kinds_directly_into_event() {
    assert!(matches!(
        kind(kind::ACCEPT, 42, MORE),
        EventKind::Accept(token, true, AcceptEvent::Accepted(fd))
            if token.route() == 7 && fd.raw() == 42
    ));
    assert!(matches!(
        kind(kind::RECV, 123, BUFFER | (17 << BUFFER_SHIFT)),
        EventKind::Recv(token, false, RecvEvent::Data { len: 123, bid: 17 })
            if token.route() == 7
    ));
    assert!(matches!(
        kind(kind::RECV_DISCARD, 31, MORE),
        EventKind::Recv(_, true, RecvEvent::Discarded { len: 31 })
    ));
    assert!(matches!(
        kind(kind::SEND, 29, 0),
        EventKind::Send(_, SendEvent::Sent(29))
    ));
    assert!(matches!(
        kind(kind::WRITE, -libc::EPIPE, 0),
        EventKind::Write(_, WriteEvent::Failed(libc::EPIPE))
    ));
    assert!(matches!(
        kind(kind::SYNC, 0, 0),
        EventKind::Sync(_, SyncEvent::Synced)
    ));
    assert!(matches!(
        kind(kind::OPEN, 51, 0),
        EventKind::Open(_, OpenEvent::Opened(51))
    ));
    assert!(matches!(
        kind(kind::READ, 8, 0),
        EventKind::Read(_, ReadEvent::Read(8))
    ));
    assert!(matches!(
        kind(kind::READ_BLOCK, 0, 0),
        EventKind::ReadBlock(_, ReadEvent::Eof)
    ));
    assert!(matches!(
        kind(kind::SPLICE, -libc::EIO, 0),
        EventKind::Splice(_, SpliceEvent::Failed(libc::EIO))
    ));
    assert!(matches!(
        kind(kind::STAT, -libc::ENOENT, 0),
        EventKind::Stat(_, StatEvent::Failed(libc::ENOENT))
    ));
    assert!(matches!(kind(kind::TIMER, 0, 0), EventKind::Timer(_)));
    assert!(matches!(
        kind(kind::SOCKET, 0, 0),
        EventKind::Socket(_, SocketEvent::Created)
    ));
    assert!(matches!(
        kind(kind::CONNECT, -libc::ECONNREFUSED, 0),
        EventKind::Connect(_, ConnectEvent::Failed(error))
            if error.raw_os_error() == Some(libc::ECONNREFUSED)
    ));
}

#[test]
fn decode_preserves_result_boundaries_and_compatibility_conversion() {
    assert!(matches!(
        kind(kind::ACCEPT, -libc::ECANCELED, 0),
        EventKind::Accept(_, false, AcceptEvent::Failed)
    ));
    assert!(matches!(
        kind(kind::RECV, 0, 0),
        EventKind::Recv(_, false, RecvEvent::Eof)
    ));
    assert!(matches!(
        kind(kind::RECV, -libc::ECANCELED, 0),
        EventKind::Recv(_, false, RecvEvent::Cancelled)
    ));
    assert!(matches!(
        kind(kind::RECV, -libc::ENOBUFS, 0),
        EventKind::Recv(_, false, RecvEvent::Starved)
    ));
    assert!(matches!(
        kind(kind::RECV, -libc::EBADF, 0),
        EventKind::Recv(_, false, RecvEvent::Failed(libc::EBADF))
    ));
    assert!(matches!(
        kind(kind::SEND, -libc::EPIPE, 0),
        EventKind::Send(_, SendEvent::Failed(libc::EPIPE))
    ));
    assert!(matches!(
        kind(kind::SYNC, -libc::EIO, 0),
        EventKind::Sync(_, SyncEvent::Failed(libc::EIO))
    ));
    assert!(matches!(
        kind(kind::OPEN, -libc::ENOENT, 0),
        EventKind::Open(_, OpenEvent::Failed(libc::ENOENT))
    ));
    assert!(matches!(
        kind(kind::READ, -libc::EIO, 0),
        EventKind::Read(_, ReadEvent::Failed(libc::EIO))
    ));
    assert!(matches!(
        kind(kind::SPLICE, 0, 0),
        EventKind::Splice(_, SpliceEvent::Eof)
    ));
    assert!(matches!(
        kind(kind::STAT, 0, 0),
        EventKind::Stat(_, StatEvent::Done)
    ));
    assert!(matches!(
        kind(kind::SOCKET, -libc::EMFILE, 0),
        EventKind::Socket(_, SocketEvent::Failed(error))
            if error.raw_os_error() == Some(libc::EMFILE)
    ));

    let converted = EventKind::try_from(cqe(kind::TIMER, 0, 0)).unwrap();
    assert!(matches!(converted, EventKind::Timer(token) if token.route() == 7));
}

#[test]
fn decode_rejects_zero_and_unknown_tokens() {
    assert!(Event::decode(Cqe::ZERO).is_err());
    assert!(Event::decode(cqe(u8::MAX, 0, 0)).is_err());
}
