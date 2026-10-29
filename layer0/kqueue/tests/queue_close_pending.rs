pub mod common;

use std::{io::Write as _, num::NonZeroUsize, os::fd::AsFd as _, time::Duration};

use dope_kqueue::Kqueue;
use dope_sys::{
    Errno,
    io::{
        fd::Fd as _,
        reactor::ready::{self, Poll as _, delivery::Event as _},
        socket::Prepare as _,
    },
    table::{
        self, Files as _,
        change::{Changes as _, Interest as _},
        closed,
    },
};
use o3::mem::quota;

use crate::common::{
    Budget, Failure,
    slots::{
        Slots,
        inputs::{Layout, Owned, Physical},
    },
    socket::try_pair,
};

type Poll = <Kqueue as ready::Source>::Poll<Slots, Failure>;
type Files = <Kqueue as table::Source>::Files<Slots>;

#[test]
fn close_clears_edits() {
    let mut poll = Poll::try_new().expect("poll owner");
    let mut files = Files::try_new(Layout).expect("file table");
    let first = Physical::at::<0>();
    let second = Physical::at::<1>();
    let (handle, _peer) = try_pair().expect("first socket");
    files
        .install(Owned::from(first), handle)
        .expect("first owner");
    let raw = files.raw(first).expect("first descriptor");
    files.try_read(first).expect("pending read delete").delete();
    files
        .try_write(first)
        .expect("pending write delete")
        .delete();
    let (next, mut peer) = try_pair().expect("replacement socket");
    assert_eq!(files.close(first), closed::File::Closed(Ok(())));
    assert!(files.changes().is_empty());

    let reused = Kqueue::duplicate::<Errno>(next.as_fd()).expect("reused descriptor");
    let handle = Kqueue::adopt(reused).expect("replacement admission");
    files
        .install(Owned::from(second), handle)
        .expect("replacement owner");
    assert_eq!(files.raw(second), Some(raw));
    let key = NonZeroUsize::MIN;
    files
        .try_read(second)
        .expect("replacement read")
        .arm(libc::EV_ADD | libc::EV_DISPATCH, key);
    peer.write_all(b"x").expect("replacement peer write");
    let source = quota::Ledger::new(1);
    let mut budget = Budget::new(&source);
    let mut storage = Poll::storage();
    let mut events = poll
        .wait(
            &mut files,
            &mut storage,
            1,
            Some(Duration::ZERO),
            &mut budget,
        )
        .expect("replacement filter remains registered");
    assert_eq!(
        events.next().expect("replacement readiness").data(),
        key.get()
    );
    assert!(events.next().is_none());
    assert!(files.changes().is_empty());
}
