use std::{num::NonZero, ops::ControlFlow, os::fd::AsFd as _, time::Duration};

use dope_kqueue::Kqueue;
use dope_sys::{
    Buffer as _, Errno,
    engine::raw::{Dispatch, Register as _},
    io::{
        fd::Fd as _,
        reactor::{
            complete::{self, Event as _},
            read::raw::Entry as _,
            ready::{self, Backend as _, Source as _, store::State as _},
        },
    },
    memory::buffer::Pool as _,
    table::{Files as _, change::Interest as _, registry::State as _},
};
use o3::{depot::map::Plan, mem::quota};

use crate::common::{
    Budget,
    cancel::pending::insert,
    event::Capacity,
    reactor::{Engine, Event, Host, Quota, Read, Receive, Turn},
    slots::inputs::{Layout, Owned, Physical},
    socket,
};

pub mod common;

#[test]
fn blocked_receives_preserve_native_lane_progress() {
    for cap in [1, 2, 256] {
        for lane in 0..usize::from(Kqueue::LANES.get()) {
            let (mut backend, _context) = Engine::try_new(
                Receive,
                Layout,
                Plan::fixed::<2>(),
                Capacity(NonZero::<u32>::MIN.saturating_add(3)),
            )
            .expect("native engine");
            let held =
                [(); 2].map(|()| backend.state().view().recv.take().expect("receive buffer"));
            let peers = [Physical::at::<0>(), Physical::at::<1>()].map(|slot| {
                let (socket, mut peer) = socket::try_pair().expect("read socket");
                let parts = backend.state_mut().edit();
                parts
                    .files
                    .install(Owned::from(slot), socket)
                    .expect("install read");
                let file = parts.files.try_read(slot).expect("read interest");
                let key = NonZero::<usize>::MIN.saturating_add(u32::from(slot) as usize);
                let echo = insert(parts.reads, Read::recv(file.id(), key))
                    .map_err(|(_, failure)| failure)
                    .expect("read registration");
                file.arm(libc::EV_ADD | libc::EV_ENABLE | libc::EV_DISPATCH, echo);
                peer.write_all(b"r").expect("read readiness");
                peer
            });
            let changes = quota::Ledger::new(2);
            let done = quota::Ledger::new(2);
            // SAFETY: both registered sockets and their pool remain owned by this engine.
            unsafe {
                ready::raw::Poll::wait(
                    &mut backend,
                    Some(Duration::ZERO),
                    &mut Budget::new(&changes),
                    &mut Budget::new(&done),
                )
            }
            .expect("blocked native reads");
            let reads = backend.state_mut().edit().reads;
            let resumed = [
                reads.pop_resume().expect("first blocked read"),
                reads.pop_resume().expect("second blocked read"),
            ];
            assert_ne!(resumed[0], resumed[1]);
            for id in resumed {
                reads.queue_resume(id);
            }

            let (control, mut signal) = socket::try_pair().expect("control source");
            // SAFETY: control remains owned until after backend destruction below.
            unsafe { backend.register(control.as_fd()) }.expect("register control");
            for _ in 0..lane {
                backend.cursor();
            }
            let mut budget = Quota(quota::Ledger::new(cap));
            let mut delivered = 0;
            for expected in 1..=4 {
                backend
                    .state_mut()
                    .edit()
                    .files
                    .try_read(Physical::at::<1>())
                    .expect("rearm interest")
                    .arm(
                        libc::EV_ENABLE | libc::EV_DISPATCH,
                        NonZero::<usize>::MIN.saturating_add(1),
                    );
                signal.write_all(b"c").expect("control readiness");
                for _ in 0..16 {
                    budget.0.reset(cap);
                    // SAFETY: registered sockets, keys, pool and control source remain live.
                    let turn: Turn<'_> =
                        unsafe { Dispatch::<'_, Host, _, _>::new(&mut backend, &budget) };
                    let (state, held) = Dispatch::<'_, Host, _, _>::dispatch(
                        turn,
                        |packet: complete::Dequeued<Event>, _| {
                            let complete::Dequeued::Emit(event) = packet else {
                                panic!("unexpected reclamation");
                            };
                            assert_eq!(event.token(), None);
                            assert_eq!(
                                Kqueue::read::<Errno>(control.as_fd(), &mut [0])
                                    .expect("drain control"),
                                b"c"
                            );
                            delivered += 1;
                            ControlFlow::Continue(())
                        },
                    )
                    .0
                    .expect("native dispatch");
                    // SAFETY: the same turn retains all registered owners and its remaining quota.
                    let _ = unsafe {
                        dope_sys::engine::raw::Poll::commit(&mut backend, &budget, || false)
                    }
                    .expect("same-turn commit");
                    assert!(state.is_pending() && held.is_none());
                    if delivered == expected && !backend.has_changes() {
                        break;
                    }
                }
                assert_eq!(delivered, expected, "cap {cap}, lane {lane}");
                assert!(
                    !backend.has_changes(),
                    "change progress: cap {cap}, lane {lane}"
                );
            }

            let [first, second] = held;
            first.recycle();
            let mut received = 0;
            for _ in 0..16 {
                budget.0.reset(cap);
                // SAFETY: the returned buffer and both live reads retain the same engine.
                let turn: Turn<'_> =
                    unsafe { Dispatch::<'_, Host, _, _>::new(&mut backend, &budget) };
                let (_, held) = Dispatch::<'_, Host, _, _>::dispatch(
                    turn,
                    |packet: complete::Dequeued<Event>, _| {
                        let complete::Dequeued::Emit(event) = packet else {
                            panic!("unexpected reclamation");
                        };
                        drop(<Event as complete::Map>::map(event, |buffer| {
                            assert_eq!(buffer.len(), 1);
                            received += 1;
                            buffer.recycle();
                        }));
                        ControlFlow::Continue(())
                    },
                )
                .0
                .expect("returned receive buffer");
                // SAFETY: the same turn retains all registered owners and its remaining quota.
                let _ =
                    unsafe { dope_sys::engine::raw::Poll::commit(&mut backend, &budget, || false) }
                        .expect("same-turn commit");
                assert!(held.is_none());
                if received == 2 {
                    break;
                }
            }
            assert_eq!(received, 2, "buffer return: cap {cap}, lane {lane}");
            second.recycle();
            backend.quiesce().expect("quiesce native reads");
            drop(backend);
            drop(control);
            drop(peers);
        }
    }
}
