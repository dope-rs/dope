use crate::common::slots::inputs::Layout;
pub mod common;

use std::{array, iter, num::NonZeroUsize, os::fd::AsRawFd as _, time::Duration};

use dope_kqueue::Kqueue;
use dope_sys::{
    Errno,
    io::{
        reactor::{
            complete::Queue as _,
            read::{self, Arm as _, raw::Entry as _},
            ready::{self, Failures, Poll as _},
        },
        socket::accept,
    },
    table::{
        self, Files as _,
        change::{
            self, Changes as _, Delete as _, Id as _, Interest as _, Tail as _, raw::Batch as _,
        },
        registry::{self, Entry as _, Occupied as _, State as _},
    },
};
use o3::{depot::map::Plan, mem::quota};

use crate::common::{
    Budget, Failure,
    cancel::{
        Context, Event,
        pending::{Pending, insert},
    },
    slots::{Slots, inputs::Physical},
};

type Poll = <Kqueue as ready::Source>::Poll<Slots, Failure>;
type Files = <Kqueue as table::Source>::Files<Slots>;
type Entry = <Kqueue as read::Source>::Entry<Context>;
type Registry = <Kqueue as read::Source>::Registry<Context>;
type Id = <Entry as registry::Entry>::File;
type Arm<'a> = <Kqueue as read::Source>::Arm<'a, Context, Event, Poll>;

#[test]
fn accept_keeps_registration_cause() {
    for cause in [
        registry::Failure::Slot,
        registry::Failure::TargetOccupied,
        registry::Failure::TargetFull,
        registry::Failure::FileOccupied,
    ] {
        let mut pending = Pending::try_new().expect("pending fixture");
        let key = if cause == registry::Failure::TargetOccupied {
            NonZeroUsize::MIN
        } else {
            NonZeroUsize::MIN.saturating_add(2)
        };
        if cause == registry::Failure::Slot {
            pending.reads = Registry::try_new(Plan::fixed::<1>()).expect("short registry");
        } else if cause == registry::Failure::TargetFull {
            let id = Id::new(Physical::at::<0>(), pending.peer.as_raw_fd());
            assert!(
                insert(
                    &mut pending.reads,
                    Entry::recv(id, NonZeroUsize::MIN.saturating_add(1)),
                )
                .is_ok()
            );
        }
        let before = pending.reads.files().collect::<Vec<_>>();
        Arm::new(&mut pending.files, &mut pending.reads, &mut pending.poll).accept_many(
            key,
            Physical::at::<1>(),
            pending.queue.vacant().expect("completion vacancy"),
        );
        let event = Event::try_emit(pending.queue.pop_front().expect("registration failure"))
            .expect("emitted terminal");
        assert_eq!(event.key, key);
        assert!(!event.more);
        assert!(
            matches!(event.result, Err(accept::Error::Registration(actual)) if actual == cause)
        );
        assert!(pending.queue.is_empty());
        assert_eq!(pending.reads.files().collect::<Vec<_>>(), before);
    }
}

#[test]
fn prepare_rolls() {
    let mut pending = Pending::try_new().expect("pending fixture");
    let mut reads = Registry::try_new(Plan::fixed::<4>()).expect("registry storage");
    let file = pending
        .files
        .raw(Physical::at::<1>())
        .expect("owned descriptor");
    let key = NonZeroUsize::MIN;
    let id = Id::new(Physical::at::<1>(), file);
    assert!(matches!(insert(&mut reads, Entry::recv(id, key)), Ok(actual) if actual == key));
    {
        let entries = reads.files().map(|id| (id, change::Filter::Read));
        let write = (
            Id::new(Physical::at::<3>(), pending.peer.as_raw_fd()),
            change::Filter::Write,
        );
        let result = pending.files.try_delete(entries.chain(iter::once(write)));
        assert!(matches!(
            &result,
            Err(error)
            if error.into_parts() == (Physical::at::<3>().index(), change::Filter::Write)
        ));
    }
    assert_eq!(reads.find(key, read::Family::Recv), Some(id));
    let tail = pending.files.edit().tail(8);
    assert_eq!(tail.as_slice().len(), 1);
    assert_eq!(tail.as_slice()[0].flags, libc::EV_ADD | libc::EV_DISPATCH);
    assert_eq!({ tail.as_slice()[0].ident }, file as libc::uintptr_t);
    tail.retry();
    assert!(pending.is_pending());
}

#[test]
fn plan_commits() {
    let mut pending = Pending::try_new().expect("pending fixture");
    let file = pending
        .files
        .raw(Physical::at::<1>())
        .expect("owned descriptor");
    let plan = {
        let entries = [Entry::recv(
            Id::new(Physical::at::<1>(), file),
            NonZeroUsize::MIN,
        )];
        let reads = entries
            .iter()
            .map(|entry| (entry.file(), change::Filter::Read));
        let write = (Id::new(Physical::at::<1>(), file), change::Filter::Write);
        pending
            .files
            .try_delete(reads.chain(iter::repeat_n(write, 2)))
            .expect("complete deletion plan")
    };
    plan.commit();
    let tail = pending.files.edit().tail(8);
    assert_eq!(tail.as_slice().len(), 2);
    for (event, filter) in tail
        .as_slice()
        .iter()
        .zip([libc::EVFILT_WRITE, libc::EVFILT_READ])
    {
        assert_eq!({ event.ident }, file as libc::uintptr_t);
        assert_eq!({ event.filter }, filter);
        assert_eq!({ event.flags }, libc::EV_DELETE);
        assert_eq!({ event.fflags }, 0);
        assert_eq!({ event.data }, 0);
        assert!(event.udata.is_null());
    }
    tail.retry();
    assert!(pending.is_pending());
}

#[test]
fn remove_holds() {
    let mut pending = Pending::try_new().expect("pending fixture");
    let mut reads = Registry::try_new(Plan::fixed::<4>()).expect("registry storage");
    let raw = pending.peer.as_raw_fd();
    let id = Id::new(Physical::at::<3>(), raw);
    let key = NonZeroUsize::MIN;
    assert!(matches!(insert(&mut reads, Entry::recv(id, key)), Ok(actual) if actual == key));
    let result = reads.remove_fd(raw, &mut pending.files);
    assert_eq!(
        result,
        Err(change::Index::new(
            Physical::at::<3>().index(),
            change::Filter::Read
        ))
    );
    assert_eq!(reads.find(key, read::Family::Recv), Some(id));
    assert!(pending.is_pending());
    assert!(matches!(
        reads.try_vacant(Entry::recv(id, key.saturating_add(1))),
        Err((_, registry::Failure::FileOccupied))
    ));
}

#[test]
fn keeps_registry_identity() {
    let mut reads = Registry::try_new(Plan::fixed::<4>()).expect("registry");
    let physical = [
        Physical::at::<0>(),
        Physical::at::<1>(),
        Physical::at::<2>(),
        Physical::at::<3>(),
    ];
    let ids = physical.map(|slot| Id::new(slot, 41 + u32::from(slot) as i32));
    let keys = [1, 65, 129, 193].map(|key| NonZeroUsize::new(key).expect("key"));
    let calls = array::from_fn::<_, 4, _>(|index| Entry::recv(ids[index], keys[index]).id());
    reads
        .try_vacant(Entry::recv(ids[0], keys[0]))
        .ok()
        .expect("borrowed vacancy");
    for index in 0..4 {
        assert!(matches!(
            insert(&mut reads, Entry::recv(ids[index], keys[index])),
            Ok(actual) if actual == keys[index]
        ));
    }
    assert!(matches!(
        reads.try_vacant(Entry::recv(ids[0], keys[0])),
        Err((_, registry::Failure::TargetOccupied))
    ));
    let next = NonZeroUsize::new(257).expect("next key");
    assert!(matches!(
        reads.try_vacant(Entry::recv(ids[0], next)),
        Err((_, registry::Failure::TargetFull))
    ));
    let stale = Entry::message(ids[0], keys[0]).id();
    assert!(reads.operation(stale).is_none());
    assert!(reads.take(stale).is_none());
    reads.queue_resume(calls[0]);
    reads.queue_resume(calls[0]);
    assert!(reads.take(calls[0]).is_some());
    assert_eq!(reads.pop_resume(), None);
    assert!(matches!(
        reads.try_vacant(Entry::recv(ids[1], next)),
        Err((_, registry::Failure::FileOccupied))
    ));
    assert!(matches!(
        insert(&mut reads, Entry::message(ids[0], keys[0])),
        Ok(actual) if actual == keys[0]
    ));
    assert!(reads.operation(calls[0]).is_none());
    assert_eq!(reads.resolve(keys[0]).map(|(id, _)| id), Some(stale));
    assert!(reads.operation(stale).is_some());
    for call in &calls[1..] {
        assert!(reads.operation(*call).is_some());
        reads.queue_resume(*call);
    }
    for call in &calls[1..] {
        assert_eq!(reads.pop_resume(), Some(*call));
    }
    assert_eq!(reads.pop_resume(), None);
    reads.clear();
    assert_eq!(reads.files().count(), 0);
    assert!(reads.resolve(keys[0]).map(|(id, _)| id).is_none());
    for index in 0..4 {
        assert!(insert(&mut reads, Entry::recv(ids[index], keys[index])).is_ok());
        let occupied = reads
            .occupied(keys[index], read::Family::Recv)
            .expect("occupied read");
        assert_eq!(occupied.remove().id(), calls[index]);
        assert!(insert(&mut reads, Entry::recv(ids[index], keys[index])).is_ok());
    }
}

#[test]
fn remove_fd_reuses() {
    let mut pending = Pending::try_new().expect("pending fixture");
    let key = NonZeroUsize::MIN;
    let id = pending.reads.find(key, read::Family::Recv).expect("read");
    let call = pending.reads.resolve(key).expect("read identity").0;
    pending.reads.queue_resume(call);
    assert_eq!(
        pending.reads.remove_fd(id.fd(), &mut pending.files),
        Ok(Some(key))
    );
    assert_eq!(pending.reads.pop_resume(), None);
    assert_eq!(
        pending.reads.remove_fd(id.fd(), &mut pending.files),
        Ok(None)
    );
    assert!(insert(&mut pending.reads, Entry::recv(id, key)).is_ok());
}

#[test]
fn receipts_full() {
    let mut files = Files::try_new(Layout).expect("file table");
    let mut poll = Poll::try_new().expect("reactor owner");
    let id = Id::new(Physical::at::<1>(), -1);
    files
        .edit()
        .read(id)
        .expect("read cell")
        .arm(libc::EV_ADD, NonZeroUsize::MIN);
    files
        .edit()
        .write(id)
        .expect("write cell")
        .arm(libc::EV_ADD, NonZeroUsize::MIN);
    let Failures { first, rest } = poll
        .revoke(&mut files)
        .expect_err("invalid descriptor changes must fail");
    let write = Failure::Revoke {
        fd: usize::MAX,
        filter: change::Filter::Write,
        errno: Errno::BAD_DESCRIPTOR,
    };
    let read = Failure::Revoke {
        fd: usize::MAX,
        filter: change::Filter::Read,
        errno: Errno::BAD_DESCRIPTOR,
    };
    assert_eq!(first, write);
    assert!(rest.into_iter().flatten().eq([read]));
    assert_eq!(files.changes().len(), 2);
    assert_eq!(poll.check(), Err(write));
    let Failures { first, rest } = poll
        .revoke(&mut files)
        .expect_err("failed reactor must retain retirement failure");
    assert_eq!(first, write);
    assert!(rest.into_iter().all(|entry| entry.is_none()));
    assert_eq!(files.changes().len(), 2);
}

#[test]
fn missing_update_fails() {
    let mut files = Files::try_new(Layout).expect("file table");
    let mut poll = Poll::try_new().expect("reactor owner");
    let id = Id::new(Physical::at::<1>(), -1);
    files
        .edit()
        .read(id)
        .expect("read cell")
        .arm(libc::EV_ENABLE, NonZeroUsize::MIN);
    let failure = Failure::Revoke {
        fd: usize::MAX,
        filter: change::Filter::Read,
        errno: Errno::NOT_FOUND,
    };
    assert_eq!(
        poll.revoke(&mut files).expect_err("missing filter").first,
        failure
    );
    assert_eq!(poll.check(), Err(failure));
    assert_eq!(files.changes().len(), 1);
}

#[test]
fn mixed_error_keeps_edits() {
    let mut files = Files::try_new(Layout).expect("file table");
    let mut poll = Poll::try_new().expect("reactor owner");
    let id = Id::new(Physical::at::<1>(), -1);
    files.edit().read(id).expect("absent read cell").delete();
    files
        .edit()
        .write(id)
        .expect("invalid write cell")
        .arm(libc::EV_ADD, NonZeroUsize::MIN);
    let source = quota::Ledger::new(2);
    let mut budget = Budget::new(&source);
    let mut storage = Poll::storage();
    let failure = Failure::Wait(Errno::BAD_DESCRIPTOR);
    assert_eq!(
        poll.wait(
            &mut files,
            &mut storage,
            0,
            Some(Duration::ZERO),
            &mut budget,
        )
        .err()
        .expect("invalid update retains exact native error"),
        failure
    );
    assert_eq!(poll.check(), Err(failure));
    assert_eq!(files.changes().len(), 2);
    assert_eq!(dope_sys::Budget::remaining(&budget), 0);
}
