use std::{
    io::{ErrorKind, Read as _},
    os::unix::net::UnixStream,
    path::Path,
    task::Poll,
    time::Duration,
};

use dope_core::{
    driver::{
        life::recovery,
        result::poll::Commit,
        route::{Epoch, KeyTag, indices::SlotIndex, kind},
        settings::FileSlots,
    },
    io::{
        event::{Kind, net::accept::Outcome},
        socket::{Backlog, Unix, install::Install, peer::Query},
    },
};
use dope_test::harness::support::file::Directory;

use common::{
    driver::{Failure, Scoped as _, Wait, polling, tuning},
    rt::{Runtime, finish, recover},
};

pub mod common;

#[test]
fn accepted_uid_and_pending_retirement() {
    let directory = Directory::try_with(Path::new("/tmp"), "unix_peer", common::file::cleanup)
        .expect("socket directory");
    let path = directory.path().join("admin");
    let address = Unix::from_path(&path).expect("Unix address");
    // SAFETY: geteuid has no preconditions and observes this process's identity.
    let expected = unsafe { libc::geteuid() };

    Runtime::quic::<2, 8>()
        .file_slots(FileSlots::fixed::<1, 1>())
        .with_scope(
            |scope| {
                scope.with_flights::<KeyTag<0>, 2>(|mut driver, turn, flights| {
                    let mut listener = Install::bind_unix(
                        driver.reborrow(),
                        &address,
                        Backlog::fixed::<2>(),
                    )
                    .expect("Unix listener");
                    let target = driver
                        .driver_ref()
                        .targets::<KeyTag<0>>()
                        .bind(SlotIndex::ZERO, Epoch::INITIAL);
                    let accepting = flights.accept(
                        driver.reborrow(),
                        &listener,
                        target.operation::<kind::Accept>(),
                    )
                    .expect("native Unix accept");

                    for (round, abandon) in [false, true, false].into_iter().enumerate() {
                        let mut peer = UnixStream::connect(&path).expect("Unix peer");
                        let done = tuning::wait(&mut driver, turn)
                            .map_err(tuning::diagnostic)
                            .expect("native accept event");
                        let done = match done {
                            Kind::Accept(done) => Some(done),
                            _ => None,
                        }
                        .expect("accept completion");
                        let (token, _, outcome) = done.into_parts();
                        assert!(accepting.matches(token));
                        let fd = match outcome {
                            Outcome::Accepted(slot) => Ok(slot.bind()),
                            Outcome::Failed(error) => Err(error),
                        }
                        .expect("same accepted fd");
                        let index = fd.index();
                        let query = Query::try_new(&mut driver, &flights, target, fd)
                            .map_err(|error| error.into_parts().1)
                            .expect("peer query");
                        match query {
                            Query::Ready(fd, uid) => {
                                assert_eq!(uid, Some(expected));
                                fd.close(&mut driver);
                            }
                            Query::Pending(pending) if abandon => {
                                drop(pending);
                                let done = tuning::wait(&mut driver, turn)
                                    .map_err(tuning::diagnostic)
                                    .expect("abandoned query terminal");
                                assert!(matches!(done, Kind::Tuning(_)));
                            }
                            Query::Pending(pending) => {
                                if round == 0 {
                                    let listener_index = listener.index();
                                    let error = Query::try_new(
                                        &mut driver, &flights, target, listener,
                                    )
                                    .map(|_| ())
                                    .expect_err("both native flights are occupied");
                                    let (returned, cause) = error.into_parts();
                                    assert!(matches!(
                                        cause,
                                        dope_core::io::socket::pending::Cause::Submit(_)
                                    ));
                                    assert_eq!(returned.index(), listener_index);
                                    listener = returned;
                                }
                                let done = tuning::wait(&mut driver, turn)
                                    .map_err(tuning::diagnostic)
                                    .expect("peer query terminal");
                                let done = match done {
                                    Kind::Tuning(done) => Some(done),
                                    _ => None,
                                }
                                .expect("query completion");
                                let (fd, uid) = pending
                                    .complete(done)
                                    .map_err(|failure| {
                                        let (_, done) = failure.into_parts();
                                        let token = done.token();
                                        (token.route(), token.slot(), token.epoch())
                                    })
                                    .expect("exact query flight");
                                assert_eq!(uid, Ok(Some(expected)));
                                assert_eq!(fd.index(), index);
                                fd.close(&mut driver);
                            }
                        }
                        polling::try_run(Duration::from_secs(1), |left| {
                            driver.commit(turn.reactor()).map(|progress| match progress {
                                Commit::Drained => Poll::Ready(()),
                                Commit::Pending => {
                                    Wait {
                                        driver: &mut driver,
                                        turn,
                                        timeout: left,
                                    }
                                    .try_run()
                                    .as_ref()
                                    .map_err(Failure::cause)
                                    .expect("retirement progress");
                                    Poll::Pending
                                }
                            })
                        })
                        .expect("submit retirement");
                        let read = polling::try_run(Duration::from_secs(1), |left| {
                            peer.set_read_timeout(Some(left))?;
                            match peer.read(&mut [0]) {
                                Ok(count) => Ok(Poll::Ready(count)),
                                Err(error) if error.kind() == ErrorKind::Interrupted => {
                                    Ok(Poll::Pending)
                                }
                                Err(error) => Err(error),
                            }
                        })
                        .expect("peer EOF");
                        assert_eq!(read, 0);
                    }
                    drop(accepting);
                    listener.close(&mut driver);
                })
                .expect("native flight storage");
            },
            recovery::Policy::new(|_| {}, recover),
            finish,
        );
}
