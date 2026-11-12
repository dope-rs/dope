use std::{mem, num::NonZero};

pub mod common;

#[test]
fn watch_terminal_records_keep_exact_masks() {
    use dope_uring::watch::{self, Event, Watch};

    let watch = Watch::try_new(c".").expect("native watch");

    const HEADER: usize = mem::size_of::<libc::inotify_event>();
    let masks = [
        libc::IN_Q_OVERFLOW,
        libc::IN_MOVE_SELF,
        libc::IN_UNMOUNT | libc::IN_ISDIR,
    ];
    let mut bytes = [0; 1 + HEADER * 3];
    for (index, mask) in masks.into_iter().enumerate() {
        let offset = 1 + index * HEADER + mem::offset_of!(libc::inotify_event, mask);
        bytes[offset..offset + 4].copy_from_slice(&mask.to_ne_bytes());
    }
    // SAFETY: each complete initialized header has zero name length and a nonzero mask.
    let mut events = unsafe { watch::raw::events(&watch, &bytes[1..]) };
    assert_eq!(events.next(), Some(Event::Overflow));
    for mask in &masks[1..] {
        assert_eq!(
            events.next(),
            Some(Event::Invalidated(NonZero::new(*mask).expect("mask")))
        );
    }
    assert_eq!(events.next(), None);
}

#[test]
fn native_watch_retains_registration_and_cancels_parked_read() {
    use dope::{
        core::{
            Errno,
            driver::{
                life::{creation, domain, recovery},
                route::KeyTag,
                settings::{self, claim::Needs},
                storage::Resident,
            },
            io::fs::watch,
        },
        manifold::{app::Host, file::read},
        runtime::executor::{Executor, drive, session},
    };
    use dope_fiber::{
        abi::{self, Task},
        file::Operation,
        task::done::Once,
    };
    use dope_test::fibers;
    use o3::{buffer::storage::Owned, mem::Lease};
    use std::{fs, pin::Pin, time::Duration};

    const LIMIT: Duration = Duration::from_secs(5);
    let dir = common::Directory::try_new("watch").expect("fixture");
    fs::write(dir.path().join("data"), b"abc").expect("fixture contents");
    let native = watch::Watch::try_new(dir.c_path()).expect("watch admission");
    fs::rename(dir.path().join("data"), dir.path().join("first")).expect("bootstrap event");
    let layout = read::Layout::<KeyTag<43>>::try_new(NonZero::<u32>::MIN).expect("one request");
    let config = Needs::from_tree(&layout)
        .expect("route")
        .apply_to(settings::Config::new(
            settings::QueueLayout::fixed::<64, 128>(),
            settings::Schedule::fixed::<16, 1>(),
            settings::FileSlots::fixed::<0, 256>(),
            settings::Receive::fixed::<2, 4096>(),
            settings::Domain::ONE,
            settings::Progress::BatchedWhenSupported,
        ))
        .expect("layout");
    let mut attempt = creation::Attempt::new();
    let executor = Executor::new(&mut attempt, config)
        .map_err(|error| format!("{:?}", error.kind()))
        .expect("executor");
    executor.run(|executor| {
        executor
            .enter_with(
                |mut session| {
                    let port = layout.try_port::<read::Release<'_>>().expect("slots");
                    let reader = read::Reader::<KeyTag<43>>::try_new(&port, &mut session.driver())
                        .expect("reader");
                    let budget = session
                        .driver()
                        .storage
                        .budget(Resident::bytes(512))
                        .expect("budget");
                    let output = Owned::try_allocate(&budget, 8).expect("small output");
                    let request = read::Watch::try_new(
                        native,
                        Lease::open(read::Release::default(), output),
                        (),
                    )
                    .ok()
                    .expect("nonempty read");
                    let mut finish = session::Completion::new();
                    session
                        .with_quiesce(&mut finish, Host::new(reader), LIMIT, |mut app| {
                            let read::Completion::Watch(result) = app
                                .drive_for(
                                    LIMIT,
                                    Once::new(Operation::new(&port, read::Request::Watch(request))),
                                )
                                .expect("small read")
                                .ok()
                                .expect("admission")
                            else {
                                panic!("watch completion")
                            };
                            let (request, cause) = result.err().expect("small record").into_parts();
                            assert_eq!(cause, read::Error::System(Errno::INVALID_ARGUMENT));
                            let (native, output, ()) = request.into_parts();
                            drop(output);
                            let output = Owned::try_allocate(&budget, 512).expect("records");
                            let address = output.as_ref().as_ptr();
                            let mut request = read::Watch::try_new(
                                native,
                                Lease::open(read::Release::default(), output),
                                (),
                            )
                            .ok()
                            .expect("record capacity");
                            for name in [c"first", c"second"] {
                                let mut operation =
                                    Task::new(Operation::new(&port, read::Request::Watch(request)));
                                if name == c"second" {
                                    assert!(
                                        app.drive_for(
                                            LIMIT,
                                            Once::new(
                                                fibers::TEST.poll_once(Pin::new(&mut operation))
                                            ),
                                        )
                                        .expect("empty read admitted")
                                        .is_none()
                                    );
                                    assert!(matches!(
                                        app.drive_for(
                                            Duration::from_millis(1),
                                            Once::new(abi::Pending::<()>::new())
                                        ),
                                        Err(drive::Error::Timeout)
                                    ));
                                    fs::rename(dir.path().join("first"), dir.path().join("second"))
                                        .expect("rename");
                                }
                                let read::Completion::Watch(result) = app
                                    .drive_for(LIMIT, Once::new(Pin::new(&mut operation)))
                                    .expect("watch completes")
                                    .ok()
                                    .expect("admission")
                                else {
                                    panic!("watch completion")
                                };
                                let batch = result.ok().expect("native records");
                                let mut events = batch.events();
                                let Some(watch::Event::MovedIn(bytes)) = events.next() else {
                                    panic!("moved entry")
                                };
                                assert!(bytes.starts_with(name.to_bytes_with_nul()));
                                assert_eq!(events.next(), None);
                                request = batch.into_read();
                                assert_eq!(request.output().as_ref().as_ptr(), address);
                            }
                            let moved = dir.path().with_extension("moved");
                            fs::rename(dir.path(), &moved).expect("move parent");
                            fs::rename(&moved, dir.path()).expect("restore fixture");
                            let read::Completion::Watch(result) = app
                                .drive_for(
                                    LIMIT,
                                    Once::new(Operation::new(&port, read::Request::Watch(request))),
                                )
                                .expect("parent event")
                                .ok()
                                .expect("admission")
                            else {
                                panic!("watch completion")
                            };
                            let batch = result.ok().expect("terminal record");
                            assert_eq!(
                                batch.events().next(),
                                Some(watch::Event::Invalidated(
                                    NonZero::new(libc::IN_MOVE_SELF).expect("mask"),
                                ))
                            );
                            let (native, output, ()) = batch.into_read().into_parts();
                            drop(native);
                            let native =
                                watch::Watch::try_new(dir.c_path()).expect("new registration");
                            let request = read::Watch::try_new(native, output, ())
                                .ok()
                                .expect("reuse buffer");
                            let mut operation =
                                Task::new(Operation::new(&port, read::Request::Watch(request)));
                            assert!(
                                app.drive_for(
                                    LIMIT,
                                    Once::new(fibers::TEST.poll_once(Pin::new(&mut operation))),
                                )
                                .expect("empty read admitted")
                                .is_none()
                            );
                            assert!(matches!(
                                app.drive_for(
                                    Duration::from_millis(1),
                                    Once::new(abi::Pending::<()>::new())
                                ),
                                Err(drive::Error::Timeout)
                            ));
                            drop(operation);
                            assert_eq!(budget.resident(), 512);
                        })
                        .expect("native quiescence");
                    assert_eq!(budget.resident(), 0);
                },
                recovery::Policy::new(
                    |event| panic!("unexpected reclaim: {event:?}"),
                    |failure: recovery::Recovery<'_, '_>| -> std::convert::Infallible {
                        panic!("native retirement: {:?}", failure.cause())
                    },
                ),
            )
            .as_ref()
            .map_err(domain::Error::cause)
            .expect("driver lifecycle");
    });
    fs::remove_file(dir.path().join("second")).expect("fixture cleanup");
}
