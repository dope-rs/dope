pub mod common;

use std::{io::Write as _, num::NonZeroUsize, ops::ControlFlow, time::Duration};

use dope_kqueue::Kqueue;
use dope_sys::{
    Errno,
    engine::admit,
    io::{
        reactor::{
            complete::{Cancellation as _, Queue as _, Vacancy as _},
            read::{self, Arm as _},
            ready::{self, Poll as _, delivery},
        },
        socket::accept,
    },
    table::{
        self, Files as _,
        change::{Changes as _, Interest as _},
        closed,
        registry::State as _,
    },
};
use o3::mem::quota;

use crate::common::{
    Budget, Failure,
    cancel::{Context, Event, pending::Pending},
    slots::{
        Slots,
        inputs::{Layout, Owned, Physical},
    },
    socket::try_pair,
};

type Poll = <Kqueue as ready::Source>::Poll<Slots, Failure>;
type Files = <Kqueue as table::Source>::Files<Slots>;
type Arm<'a> = <Kqueue as read::Source>::Arm<'a, Context, Event, Poll>;

#[test]
fn full_keeps_owner() {
    let mut pending = Pending::try_new().expect("pending fixture");
    pending
        .queue
        .vacant()
        .expect("fill completion queue")
        .push(Event {
            key: NonZeroUsize::MIN.saturating_add(1),
            result: Ok(()),
            more: false,
        });
    {
        let prepared = Arm::new(&mut pending.files, &mut pending.reads, &mut pending.poll)
            .prepare(NonZeroUsize::MIN, read::Family::Recv)
            .expect("healthy preparation");
        assert!(prepared.is_some(), "registered read");
        assert!(pending.queue.vacant().is_none());
    }
    let filler = Event::try_emit(pending.queue.pop_front().expect("original completion"))
        .expect("emitted completion");
    assert_eq!(filler.key, NonZeroUsize::MIN.saturating_add(1));
    assert!(filler.result.is_ok());
    assert!(!filler.more);
    assert!(pending.is_pending());

    use delivery::Event as _;
    let quota = quota::Ledger::new(1);
    let mut budget = Budget::new(&quota);
    let mut storage = Poll::storage();
    pending.peer.write_all(b"x").expect("peer write");
    let mut events = pending
        .poll
        .wait(
            &mut pending.files,
            &mut storage,
            1,
            Some(Duration::ZERO),
            &mut budget,
        )
        .expect("retained interest must become ready");
    let event = events.next().expect("original read readiness");
    assert_eq!(event.filter(), libc::EVFILT_READ);
    assert_eq!(event.data(), NonZeroUsize::MIN.get());
    assert!(events.next().is_none());
    assert!(pending.is_pending());
}

#[test]
fn poll_keeps_owner() {
    let mut pending = Pending::try_new().expect("pending fixture");
    assert_eq!(
        pending
            .poll
            .execute(|| Err::<(), _>(Errno::BAD_DESCRIPTOR), Failure::System),
        ControlFlow::Break(())
    );
    assert!(matches!(
        pending.arm().prepare(NonZeroUsize::MIN, read::Family::Recv),
        Err(Failure::System(Errno::BAD_DESCRIPTOR))
    ));
    assert!(pending.is_pending());
    assert_eq!(
        pending.poll.check(),
        Err(Failure::System(Errno::BAD_DESCRIPTOR))
    );
}

#[test]
fn stale_completes() {
    let mut pending = Pending::try_new().expect("pending fixture");
    assert!(matches!(
        pending.files.close(Physical::at::<1>()),
        closed::File::Closed(Ok(()))
    ));
    let prepared = Arm::new(&mut pending.files, &mut pending.reads, &mut pending.poll)
        .prepare(NonZeroUsize::MIN, read::Family::Recv)
        .expect("stale generation is classified")
        .expect("registry owner remains");
    assert_eq!(
        prepared.commit(pending.queue.vacant().expect("completion vacancy")),
        admit::Submitted
    );
    assert!(
        pending
            .reads
            .find(NonZeroUsize::MIN, read::Family::Recv)
            .is_none()
    );
    assert!(pending.queue.is_terminal(NonZeroUsize::MIN));
    let event = Event::try_emit(pending.queue.pop_front().expect("canceled terminal"))
        .expect("emitted completion");
    assert_eq!(event.key, NonZeroUsize::MIN);
    assert!(!event.more);
    assert!(matches!(
        event.result,
        Err(accept::Error::Syscall(Errno::CANCELED))
    ));
    assert!(pending.queue.pop_front().is_none());
}

#[test]
fn matches_key() {
    let mut pending = Pending::try_new().expect("pending fixture");
    assert!(
        pending
            .arm()
            .prepare(NonZeroUsize::MIN.saturating_add(1), read::Family::Recv)
            .expect("healthy key lookup")
            .is_none()
    );
    assert!(
        pending
            .arm()
            .prepare(NonZeroUsize::MIN, read::Family::Accept)
            .expect("healthy family lookup")
            .is_none()
    );
    assert!(pending.is_pending());
}

#[test]
fn cancels_unpublished_full() {
    let mut pending = Pending::try_new().expect("pending fixture");
    let prepared = Arm::new(&mut pending.files, &mut pending.reads, &mut pending.poll)
        .prepare(NonZeroUsize::MIN, read::Family::Recv)
        .expect("healthy preparation")
        .expect("pending read");
    prepared.commit(pending.queue.vacant().expect("completion vacancy"));
    assert!(pending.queue.vacant().is_none());
    let source = quota::Ledger::new(1);
    let mut budget = Budget::new(&source);
    let mut storage = Poll::storage();
    assert!(
        pending
            .poll
            .wait(
                &mut pending.files,
                &mut storage,
                0,
                Some(Duration::ZERO),
                &mut budget,
            )
            .expect("absent filter deletion must complete without delivery capacity")
            .next()
            .is_none()
    );
    assert!(pending.files.changes().is_empty());
    assert_eq!(dope_sys::Budget::remaining(&budget), 0);
    assert_eq!(pending.poll.check(), Ok(()));
    assert!(pending.queue.is_terminal(NonZeroUsize::MIN));
}

#[test]
fn cancels_persistent_read() {
    let mut pending = Pending::try_new().expect("pending fixture");
    let source = quota::Ledger::new(2);
    let mut budget = Budget::new(&source);
    let mut storage = Poll::storage();
    pending
        .poll
        .wait(
            &mut pending.files,
            &mut storage,
            0,
            Some(Duration::ZERO),
            &mut budget,
        )
        .expect("install persistent dispatch filter");
    let prepared = Arm::new(&mut pending.files, &mut pending.reads, &mut pending.poll)
        .prepare(NonZeroUsize::MIN, read::Family::Recv)
        .expect("first preparation")
        .expect("first read");
    prepared.commit(pending.queue.vacant().expect("first completion"));
    pending.queue.pop_front().expect("first canceled terminal");

    let next = NonZeroUsize::MIN.saturating_add(1);
    Arm::new(&mut pending.files, &mut pending.reads, &mut pending.poll).recv(
        next,
        Physical::at::<1>(),
        pending.queue.vacant().expect("replacement admission"),
    );
    let prepared = Arm::new(&mut pending.files, &mut pending.reads, &mut pending.poll)
        .prepare(next, read::Family::Recv)
        .expect("replacement preparation")
        .expect("replacement read");
    prepared.commit(pending.queue.vacant().expect("replacement completion"));
    pending
        .poll
        .wait(
            &mut pending.files,
            &mut storage,
            0,
            Some(Duration::ZERO),
            &mut budget,
        )
        .expect("remove the previously installed filter");
    pending.peer.write_all(b"x").expect("peer write");
    assert!(
        pending
            .poll
            .wait(
                &mut pending.files,
                &mut storage,
                1,
                Some(Duration::ZERO),
                &mut budget,
            )
            .expect("no canceled generation remains registered")
            .next()
            .is_none()
    );
    assert!(pending.queue.is_terminal(next));
}

#[test]
fn receipts_keep_readiness() {
    use delivery::Event as _;

    let mut files = Files::try_new(Layout).expect("file table");
    let mut poll = Poll::try_new().expect("reactor owner");
    let (handle, _peer) = try_pair().expect("first socket");
    files
        .install(Owned::from(Physical::at::<0>()), handle)
        .expect("first owner");
    files
        .try_read(Physical::at::<0>())
        .expect("absent first filter")
        .delete();
    let (handle, mut peer) = try_pair().expect("second socket");
    files
        .install(Owned::from(Physical::at::<1>()), handle)
        .expect("second owner");
    let key = NonZeroUsize::MIN.saturating_add(1);
    files
        .try_read(Physical::at::<1>())
        .expect("second interest")
        .arm(libc::EV_ADD | libc::EV_DISPATCH, key);
    peer.write_all(b"x").expect("second peer write");
    let source = quota::Ledger::new(2);
    let mut budget = Budget::new(&source);
    let mut storage = Poll::storage();
    assert!(
        poll.wait(
            &mut files,
            &mut storage,
            0,
            Some(Duration::ZERO),
            &mut budget,
        )
        .expect("mixed changes preserve pending readiness")
        .next()
        .is_none()
    );
    assert_eq!(dope_sys::Budget::remaining(&budget), 0);
    let mut events = poll
        .wait(
            &mut files,
            &mut storage,
            1,
            Some(Duration::ZERO),
            &mut budget,
        )
        .expect("retained readiness");
    assert_eq!(events.next().expect("second readiness").data(), key.get());
    assert!(events.next().is_none());
    assert!(files.changes().is_empty());
}
