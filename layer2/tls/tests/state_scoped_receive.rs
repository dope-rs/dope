use std::ops::ControlFlow;

use dope::{
    core::driver::storage,
    net::{
        link::{
            event::{ControlRecv, DispatchRecv, Kind, ParkRecv, Reservation},
            pool::recv::{Deferral, Ready, Resumption},
        },
        wire::receive::scoped,
    },
};
use o3::buffer::{hold, resident};

use crate::common::retry::sibling;

pub mod common;

#[test]
fn staged_head_parks_provided_sibling() {
    const FIRST: &[u8] = &[b's'; 512];
    const SIBLING: &[u8] = b"provided";
    const AFTER: &[u8] = b"after";

    sibling::run(|peer, pool, scope| {
        let budget = scope.context().storage.budget(storage::Resident::bytes(4096))
            .expect("receive domain");
        let key = peer.key;
        let mut first = peer.seal(FIRST);
        let last = first.pop().expect("nonempty ciphertext");
        let (prefix, _) = peer.receive(pool, scope, &first)?;
        assert!(matches!(prefix, Reservation::Ready(DispatchRecv::Control(ControlRecv::NoChunk(k))) if k == key));

        let mut suffix = vec![last];
        suffix.extend_from_slice(&peer.seal(SIBLING));
        assert!(suffix.len() < FIRST.len());
        let (received, provided) = peer.receive(pool, scope, &suffix)?;
        let mut chunk = match received {
            Reservation::Ready(DispatchRecv::Chunk(k, chunk)) if k == key => chunk,
            _ => return Err(common::retry::Error::Record),
        };
        let mut held = None;
        let result = scoped::with(&mut chunk, &budget, |input| {
            let head = input.head().expect("staged first record");
            let input = head.input();
            assert_eq!(input.as_slice(), FIRST);
            let address = input.as_slice().as_ptr();
            let saved: resident::Shared<'_> = input.defer_all().bind(hold::Policy::share())
                .try_into().expect("retain staged head");
            assert_eq!(saved.as_slice().as_ptr(), address);
            held = Some(saved.try_owned().expect("independent staging owner"));
            head.finish().expect("advance to provided record").settle()
        }).expect("scoped receive");
        assert!(result.is_none());
        assert_eq!(chunk.as_ref().as_ptr(), provided);

        let mut entry = pool.entry(key).ok().expect("original connection");
        assert!(matches!(entry.defer(chunk), Deferral::Ready));
        let mut tail = match entry.take_tail() {
            ControlFlow::Continue(Some(taken)) => taken.commit(),
            _ => return Err(common::retry::Error::Record),
        };
        assert_eq!(tail.as_ref().as_ptr(), provided);
        assert!(scoped::with(&mut tail, &budget, |input| {
            let head = input.head().expect("parked sibling");
            assert_eq!(head.input().as_slice(), SIBLING);
            head.finish().expect("finish provided record").settle()
        }).expect("replayed input").is_some());
        drop(tail);
        assert!(matches!(entry.take_tail(), ControlFlow::Continue(None)));
        entry.finish_tail();

        let next = peer.seal(AFTER);
        let (parked, next_address) = peer.receive(pool, scope, &next)?;
        assert!(matches!(parked, Reservation::Parked(ParkRecv::Deferred)));
        sibling::flush(pool, scope)?;
        scope.split().2.begin().ready().drain(|target| assert!(key.target().dispatch().matches(target)));
        let held = held.expect("staged owner outlives sibling replay");
        assert_eq!(held.as_slice(), FIRST);
        drop(held);
        sibling::flush(pool, scope)?;
        let wake = scope.split().2.begin().ready().drain(|target| assert!(key.target().dispatch().matches(target)));
        assert_eq!(wake.map(|count| count.get()), Some(1));
        let Resumption::Ready(resumed) = pool.recv().resume(key.target()) else {
            return Err(common::retry::Error::Key);
        };
        let Ready::Completion(done) = pool.recv().pop_ready(&resumed) else {
            return Err(common::retry::Error::Record);
        };
        let Kind::Data(data) = done.classify() else {
            return Err(common::retry::Error::Control);
        };
        let (_, received) = pool.recv().data(data, |_, _| (), |plan| plan.map(|data| data.scoped()));
        assert!(matches!(received, Reservation::Ready(DispatchRecv::Chunk(k, ref chunk))
            if k == key && chunk.as_ref() == AFTER && chunk.as_ref().as_ptr() == next_address));
        assert!(matches!(pool.recv().pop_ready(&resumed), Ready::Empty));
        Ok(())
    });
}
