pub mod common;

use std::{
    io::Read as _,
    iter, mem,
    num::NonZeroUsize,
    os::fd::{AsFd as _, AsRawFd as _},
};

use dope_kqueue::Kqueue;
use dope_sys::table::{
    self, Files as _,
    change::{self, Changes as _, Interest as _, Tail as _, raw::Batch as _},
    closed::File,
};

use crate::common::{
    slots::{
        Slots,
        inputs::{Layout, Missing, Owned, Physical},
    },
    socket::{try_exchange, try_pair},
};

type Files = <Kqueue as table::Source>::Files<Slots>;
type ChangeId = <<Files as table::Files<Slots>>::Changes as change::Changes>::Id;

#[test]
fn owns_successful_install() {
    let mut files = Files::try_new(Layout).expect("file table must allocate");
    let slot = Physical::at::<1>();
    let (handle, mut peer) = try_pair().expect("socket pair");
    let raw = handle.as_fd().as_raw_fd();
    assert!(files.install(Owned::from(slot), handle).is_ok());
    assert_eq!(files.raw(slot), Some(raw));
    assert_eq!(
        try_exchange(&files, slot, &mut peer).expect("connected peer"),
        *b"x"
    );
    assert_eq!(files.close(slot), File::Closed(Ok(())));
    assert_eq!(files.raw(slot), None);
    assert_eq!(
        files.close(slot),
        File::Missing(Missing::Vacant {
            index: slot.index()
        })
    );
    assert_eq!(
        peer.read(&mut [0]).expect("closed owner must deliver EOF"),
        0
    );
}

#[test]
fn rejects_replacement() {
    let slot = Physical::at::<1>();
    let (first, mut peer) = try_pair().expect("socket pair");
    {
        let mut files = Files::try_new(Layout).expect("file table must allocate");
        let raw = first.as_fd().as_raw_fd();
        assert!(files.install(Owned::from(slot), first).is_ok());

        let (second, mut rejected) = try_pair().expect("socket pair");
        assert!(matches!(files.install(Owned::from(slot), second),
            Err(table::Install::Occupied(index)) if index == slot.index()));
        assert_eq!(
            rejected
                .read(&mut [0])
                .expect("closed owner must deliver EOF"),
            0
        );
        assert_eq!(files.raw(slot), Some(raw));
        assert_eq!(
            try_exchange(&files, slot, &mut peer).expect("connected peer"),
            *b"x"
        );

        let outside = Physical::at::<2>();
        let (third, mut rejected) = try_pair().expect("socket pair");
        assert!(matches!(files.install(Owned::from(outside), third),
            Err(table::Install::Index(index)) if index == outside.index()));
        assert_eq!(
            rejected
                .read(&mut [0])
                .expect("closed owner must deliver EOF"),
            0
        );
        assert_eq!(files.raw(outside), None);
        assert_eq!(files.raw(slot), Some(raw));
        assert_eq!(
            try_exchange(&files, slot, &mut peer).expect("connected peer"),
            *b"x"
        );
    }
    assert_eq!(
        peer.read(&mut [0]).expect("closed owner must deliver EOF"),
        0
    );
}

#[test]
fn keeps_accepted_prefix() {
    let mut spare = {
        let mut files = Files::try_new(Layout).expect("file table must allocate");
        let (first, mut peer) = try_pair().expect("socket pair");
        let accepted = files
            .establish(first)
            .expect("accepted prefix must have one free row");
        assert_eq!(accepted.physical(), Physical::at::<0>());

        let (second, mut spare) = try_pair().expect("socket pair");
        let second = files
            .establish(second)
            .expect_err("outbound rows must stay reserved");
        assert_eq!(files.raw(Physical::at::<1>()), None);
        assert_eq!(files.close(accepted.physical()), File::Closed(Ok(())));
        assert_eq!(
            peer.read(&mut [0]).expect("closed owner must deliver EOF"),
            0
        );
        let accepted = files
            .establish(second)
            .expect("closing the accepted row must free it");
        assert_eq!(accepted.physical(), Physical::at::<0>());
        assert_eq!(
            try_exchange(&files, accepted.physical(), &mut spare).expect("connected peer"),
            *b"x"
        );
        spare
    };
    assert_eq!(
        spare.read(&mut [0]).expect("closed owner must deliver EOF"),
        0
    );
}

#[test]
fn rejects_stale_identity() {
    let mut files = Files::try_new(Layout).expect("file table must allocate");
    let slot = Physical::at::<1>();
    let (first, mut peer) = try_pair().expect("socket pair");
    let expected = first.as_fd().as_raw_fd();
    files
        .install(Owned::from(slot), first)
        .expect("first socket");
    let id = files.try_read(slot).expect("registered identity").id();
    assert_eq!(
        files
            .try_socket(id)
            .expect("live identity")
            .as_fd()
            .as_raw_fd(),
        expected
    );

    let (second, mut next) = try_pair().expect("socket pair");
    let actual = second.as_fd().as_raw_fd();
    assert_ne!(actual, expected);
    assert_eq!(files.close(slot), File::Closed(Ok(())));
    assert_eq!(
        peer.read(&mut [0]).expect("closed owner must deliver EOF"),
        0
    );
    assert_eq!(
        files.try_socket(id).expect_err("vacant identity"),
        Missing::Vacant {
            index: slot.index()
        }
    );
    files
        .install(Owned::from(slot), second)
        .expect("replacement socket");
    assert_eq!(
        files.try_socket(id).expect_err("replaced identity"),
        Missing::Replaced {
            index: slot.index(),
            expected,
            actual
        },
    );
    assert_eq!(
        try_exchange(&files, slot, &mut next).expect("connected peer"),
        *b"x"
    );
    assert_eq!(files.close(slot), File::Closed(Ok(())));
    assert_eq!(
        next.read(&mut [0]).expect("closed owner must deliver EOF"),
        0
    );
}

#[test]
fn commits_staged_changes() {
    let mut files = Files::try_new(Layout).expect("file table");
    let first = Physical::at::<0>();
    let second = Physical::at::<1>();
    let (handle, mut peer) = try_pair().expect("socket pair");
    files
        .install(Owned::from(first), handle)
        .expect("first owner");
    let (handle, mut next) = try_pair().expect("socket pair");
    files
        .install(Owned::from(second), handle)
        .expect("second owner");
    for slot in [first, second] {
        files
            .try_read(slot)
            .expect("read interest")
            .arm(libc::EV_ADD, NonZeroUsize::MIN);
    }
    files
        .try_write(second)
        .expect("write interest")
        .arm(libc::EV_ADD, NonZeroUsize::MIN.saturating_add(1));
    assert_eq!(files.changes().len(), 3);

    let first_fd = files.raw(first).expect("first descriptor") as libc::uintptr_t;
    let next_fd = files.raw(second).expect("second descriptor") as libc::uintptr_t;
    let mut tail = files.edit().tail(1);
    assert_eq!(tail.as_slice().len(), 1);
    assert_eq!({ tail.as_slice()[0].filter }, libc::EVFILT_WRITE);
    tail.as_mut_slice()[0].flags |= libc::EV_RECEIPT;
    tail.as_mut_slice()[0].ident = first_fd;
    tail.as_mut_slice()[0].filter = libc::EVFILT_READ;
    tail.commit();
    assert_eq!(files.changes().len(), 2);

    let tail = files.edit().tail(1);
    assert_eq!({ tail.as_slice()[0].ident }, next_fd);
    assert_eq!({ tail.as_slice()[0].filter }, libc::EVFILT_READ);
    tail.retry();
    assert_eq!(files.changes().len(), 2);
    let tail = files.edit().tail(2);
    assert!(
        tail.as_slice()
            .iter()
            .map(|event| (event.ident, event.filter))
            .eq([(next_fd, libc::EVFILT_READ), (first_fd, libc::EVFILT_READ),])
    );
    tail.commit();
    assert!(files.changes().is_empty());
    assert_eq!(
        try_exchange(&files, first, &mut peer).expect("connected peer"),
        *b"x"
    );
    assert_eq!(
        try_exchange(&files, second, &mut next).expect("connected peer"),
        *b"x"
    );
}

#[test]
fn keeps_prefix_occupied() {
    let mut files = Files::try_new(Layout).expect("file table");
    let first = Physical::at::<0>();
    let (handle, mut peer) = try_pair().expect("socket pair");
    files
        .install(Owned::from(first), handle)
        .expect("prefix owner");
    let (handle, mut next) = try_pair().expect("socket pair");
    let handle = files
        .establish(handle)
        .expect_err("installed prefix is occupied");
    assert_eq!(
        try_exchange(&files, first, &mut peer).expect("connected peer"),
        *b"x"
    );
    assert_eq!(files.close(first), File::Closed(Ok(())));
    let accepted = files.establish(handle).expect("closed prefix becomes free");
    assert_eq!(accepted.physical(), first);
    assert_eq!(
        try_exchange(&files, first, &mut next).expect("connected peer"),
        *b"x"
    );
}

#[test]
fn counts_pending_changes() {
    use change::{Delete as _, Filter, Write as _};

    let mut files = Files::try_new(Layout).expect("file table");
    let slot = Physical::at::<1>();
    let (handle, mut peer) = try_pair().expect("socket pair");
    files.install(Owned::from(slot), handle).expect("owner");
    let id = files.try_read(slot).expect("read identity").id();
    for _ in 0..2 {
        files
            .try_read(slot)
            .expect("read")
            .arm(libc::EV_ADD, NonZeroUsize::MIN);
    }
    assert_eq!(files.changes().len(), 1);
    files.try_write(slot).expect("write").cancel();
    assert_eq!(files.changes().len(), 2);
    files.try_write(slot).expect("write").cancel();
    assert_eq!(files.changes().len(), 1);
    {
        let deletion = files.try_delete([(id, Filter::Read), (id, Filter::Write)].into_iter());
        assert!(
            deletion.is_ok(),
            "uncommitted deletion: {:?}",
            deletion.as_ref().err()
        );
    }
    let tail = files.edit().tail(usize::MAX);
    assert_eq!(tail.len(), 1);
    assert_eq!({ tail.as_slice()[0].flags }, libc::EV_ADD);
    tail.retry();
    files.try_read(slot).expect("read").discard();
    assert!(files.changes().is_empty());

    files
        .try_delete([(id, Filter::Read), (id, Filter::Write), (id, Filter::Read)].into_iter())
        .expect("deduplicated deletion")
        .commit();
    assert_eq!(files.changes().len(), 2);
    {
        let tail = files.edit().tail(1);
        assert_eq!(tail.len(), 1);
    }
    assert_eq!(files.changes().len(), 2);
    files.edit().tail(0).commit();
    assert_eq!(files.changes().len(), 2);
    files.edit().tail(1).retry();
    assert_eq!(files.changes().len(), 2);
    let tail = files.edit().tail(usize::MAX);
    assert_eq!(tail.len(), 2);
    assert_eq!({ tail.as_slice()[0].filter }, libc::EVFILT_WRITE);
    assert_eq!({ tail.as_slice()[1].filter }, libc::EVFILT_READ);
    tail.commit();
    assert!(files.changes().is_empty());
    assert_eq!(
        try_exchange(&files, slot, &mut peer).expect("connected peer"),
        *b"x"
    );
}

#[test]
fn rejects_plan_reuse() {
    use change::{Delete as _, Filter, Id as _};

    let mut files = Files::try_new(Layout).expect("file table");
    let id = ChangeId::new(Physical::at::<1>(), 41);
    files
        .edit()
        .read(id)
        .expect("represented cell")
        .arm(libc::EV_ADD, NonZeroUsize::MIN);
    let error = change::Index::new(Physical::at::<3>().index(), Filter::Read);
    assert!(matches!(
        files.edit().try_delete([Ok(Some((id, Filter::Read))), Err(error)].into_iter()),
        Err(actual) if actual == error
    ));
    let tail = files.edit().tail(1);
    assert_eq!({ tail.as_slice()[0].flags }, libc::EV_ADD);
    tail.retry();
    files
        .edit()
        .try_delete(iter::empty())
        .expect("empty plan")
        .commit();
    assert!(files.changes().is_empty());
}

#[test]
fn forgets_plan_reuse() {
    use change::{Delete as _, Filter, Id as _};

    let mut files = Files::try_new(Layout).expect("file table");
    let id = ChangeId::new(Physical::at::<1>(), 41);
    let plan = files
        .edit()
        .try_delete([Ok(Some((id, Filter::Write)))].into_iter())
        .expect("deletion plan");
    mem::forget(plan);
    files
        .edit()
        .write(id)
        .expect("represented cell")
        .arm(libc::EV_ADD, NonZeroUsize::MIN);
    files.edit().tail(1).retry();
    files
        .edit()
        .try_delete(iter::empty())
        .expect("new plan")
        .commit();
    assert!(files.changes().is_empty());
}
