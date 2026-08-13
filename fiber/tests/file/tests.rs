use std::path::Path;

use dope::{
    core::io::fs::{Directory, Native, OpenPath},
    manifold::file::{Access, Manifold, Regular},
};
use dope_fiber::{
    abi::batch::{Batch, Domain},
    file::{OpenRegular, ReadAll},
};
use dope_test::{fibers, file, scenario::rt::Runtime};

const ID: u8 = 7;

fn confined(path: &Path) -> OpenPath {
    let parent = path.parent().expect("file parent");
    let name = path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .expect("utf-8 file name");
    Directory::open(parent)
        .expect("directory capability")
        .relative(name)
        .expect("relative file path")
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
struct Host<'d> {
    #[pin]
    #[manifold]
    files: Manifold<'d, ID, 64, Native>,
    #[dispatcher(marker)]
    driver: ::core::marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

type AppSession<'app, 'd> = dope::runtime::executor::session::Application<'app, 'd, Host<'d>>;

#[test]
fn storage_route_can_be_installed_by_sequential_apps() {
    Runtime::throughput()
        .files::<ID, 64, Native>()
        .try_enter(|mut session| {
            let files = session.storage();
            for _ in 0..2 {
                session
                    .with_app(
                        Host {
                            files: files.manifold(),
                            driver: ::core::marker::PhantomData,
                        },
                        |_app| {},
                    )
                    .expect("sequential file owner teardown");
            }
        })
        .expect("file storage");
}

fn open_regular<'app, 'd: 'app>(
    app: &mut AppSession<'app, 'd>,
    access: Access<'app, 'd, ID, 64, Native>,
    path: OpenPath,
) -> Regular {
    fibers::TEST
        .drive(app, OpenRegular::new(access, path))
        .expect("open regular")
}

fn read_all<'app, 'd: 'app>(
    app: &mut AppSession<'app, 'd>,
    access: Access<'app, 'd, ID, 64, Native>,
    file: Regular,
) -> Vec<u8> {
    let read = ReadAll::try_new(access, file)
        .map_err(|(_, error)| error)
        .expect("read buffer");
    fibers::TEST.drive(app, read).expect("read all")
}

#[test]
fn regular_file_carries_same_descriptor_metadata_and_reads_all() {
    let payload = b"awaited-through-the-runtime";
    let source = file::File::with("open_read", payload);
    Runtime::throughput()
        .files::<ID, 64, Native>()
        .try_enter(|mut sess| {
            let files = sess.storage();
            sess.with_app(
                Host {
                    files: files.manifold(),
                    driver: ::core::marker::PhantomData,
                },
                |mut app| {
                    let access = app.client(|host| host.project_ref().files);
                    let file = open_regular(&mut app, access, confined(source.path()));
                    assert_eq!(file.metadata().len(), payload.len() as u64);
                    assert_eq!(read_all(&mut app, access, file), payload);
                },
            )
            .expect("application teardown");
        })
        .expect("file storage");
}

#[test]
fn read_uses_the_verified_descriptor_after_path_replacement() {
    let original = b"verified-descriptor";
    let source = file::File::with("same_fd", original);
    let moved = source.path().with_extension("opened");
    Runtime::throughput()
        .files::<ID, 64, Native>()
        .try_enter(|mut sess| {
            let files = sess.storage();
            sess.with_app(
                Host {
                    files: files.manifold(),
                    driver: ::core::marker::PhantomData,
                },
                |mut app| {
                    let access = app.client(|host| host.project_ref().files);
                    let file = open_regular(&mut app, access, confined(source.path()));
                    std::fs::rename(source.path(), &moved).expect("move opened path");
                    std::fs::write(source.path(), b"replacement").expect("replace path");
                    assert_eq!(read_all(&mut app, access, file), original);
                },
            )
            .expect("application teardown");
        })
        .expect("file storage");
    let _ = std::fs::remove_file(moved);
}

#[test]
fn empty_regular_file_reads_without_a_kernel_read() {
    let source = file::File::with("empty_read", b"");
    Runtime::throughput()
        .files::<ID, 64, Native>()
        .try_enter(|mut sess| {
            let files = sess.storage();
            sess.with_app(
                Host {
                    files: files.manifold(),
                    driver: ::core::marker::PhantomData,
                },
                |mut app| {
                    let access = app.client(|host| host.project_ref().files);
                    let file = open_regular(&mut app, access, confined(source.path()));
                    assert!(read_all(&mut app, access, file).is_empty());
                },
            )
            .expect("application teardown");
        })
        .expect("file storage");
}

#[test]
fn batch_open_wakes_each_operation() {
    let first = file::File::with("batch_first", b"first");
    let second = file::File::with("batch_second", b"second");
    Runtime::throughput()
        .files::<ID, 64, Native>()
        .try_enter(|mut sess| {
            let files = sess.storage();
            sess.with_app(
                Host {
                    files: files.manifold(),
                    driver: ::core::marker::PhantomData,
                },
                |mut app| {
                    let access = app.client(|host| host.project_ref().files);
                    let opens = dope_gen::fiber!('_, crate = ::dope_fiber => async move {
                        let mut domain = Domain::<2>::acquire()
                            .await
                            .expect("batch ready domain");
                        Batch::try_from_array(
                            &mut domain,
                            [
                                OpenRegular::new(access, confined(first.path())),
                                OpenRegular::new(access, confined(second.path())),
                            ],
                        )
                        .expect("batch queue allocation")
                        .await
                    });
                    let files = fibers::TEST.drive(&mut app, opens).collect::<Vec<_>>();
                    assert_eq!(files.len(), 2);
                    assert!(files.into_iter().all(|file| file.is_ok()));
                },
            )
            .expect("application teardown");
        })
        .expect("file storage");
}

#[test]
fn missing_and_non_regular_paths_are_rejected() {
    let root = file::Directory::with("regular_only_root");
    std::fs::create_dir(root.path().join("directory")).expect("nested directory");
    let directory = Directory::open(root.path()).expect("directory capability");

    Runtime::throughput()
        .files::<ID, 64, Native>()
        .try_enter(|mut sess| {
            let files = sess.storage();
            sess.with_app(
                Host {
                    files: files.manifold(),
                    driver: ::core::marker::PhantomData,
                },
                |mut app| {
                    let access = app.client(|host| host.project_ref().files);
                    let missing = directory.relative("missing").expect("missing path");
                    let error = fibers::TEST
                        .drive(&mut app, OpenRegular::new(access, missing))
                        .expect_err("missing path");
                    assert_eq!(error.raw_os_error(), Some(libc::ENOENT));

                    let nested = directory.relative("directory").expect("directory path");
                    let error = fibers::TEST
                        .drive(&mut app, OpenRegular::new(access, nested))
                        .expect_err("directory is not a regular file");
                    assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
                },
            )
            .expect("application teardown");
        })
        .expect("file storage");
}

#[test]
fn fifo_does_not_block_file_lane_or_following_regular_open() {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};

    let root = file::Directory::with("fifo_regular_only_root");
    let fifo = root.path().join("pipe");
    let fifo_path = CString::new(fifo.as_os_str().as_bytes()).expect("fifo path");
    assert_eq!(unsafe { libc::mkfifo(fifo_path.as_ptr(), 0o600) }, 0);
    std::fs::write(root.path().join("file"), b"after-fifo").expect("regular file");
    let directory = Directory::open(root.path()).expect("directory capability");

    Runtime::throughput()
        .files::<ID, 64, Native>()
        .try_enter(|mut sess| {
            let files = sess.storage();
            sess.with_app(
                Host {
                    files: files.manifold(),
                    driver: ::core::marker::PhantomData,
                },
                |mut app| {
                    let access = app.client(|host| host.project_ref().files);
                    let pipe = directory.relative("pipe").expect("fifo path");
                    let error = fibers::TEST
                        .drive(&mut app, OpenRegular::new(access, pipe))
                        .expect_err("fifo is not regular");
                    assert_eq!(error.kind(), std::io::ErrorKind::NotFound);

                    let regular = directory.relative("file").expect("regular path");
                    let file = fibers::TEST
                        .drive(&mut app, OpenRegular::new(access, regular))
                        .expect("file lane remains live");
                    assert_eq!(read_all(&mut app, access, file), b"after-fifo");
                },
            )
            .expect("application teardown");
        })
        .expect("file storage");
}

#[test]
fn directory_open_confines_symlinks_to_its_root() {
    use std::{fs, os::unix::fs::symlink};

    let root = file::Directory::with("confined_root");
    let outside = file::Directory::with("confined_outside");
    fs::create_dir(root.path().join("static")).expect("static directory");
    fs::write(root.path().join("static/inside.txt"), b"inside").expect("inside file");
    fs::write(outside.path().join("secret.txt"), b"outside file").expect("outside file");
    symlink(
        outside.path().join("secret.txt"),
        root.path().join("static/outside-file.txt"),
    )
    .expect("file symlink");
    let directory = Directory::open(root.path()).expect("directory capability");

    Runtime::throughput()
        .files::<ID, 64, Native>()
        .try_enter(|mut sess| {
            let files = sess.storage();
            sess.with_app(
                Host {
                    files: files.manifold(),
                    driver: ::core::marker::PhantomData,
                },
                |mut app| {
                    let access = app.client(|host| host.project_ref().files);
                    let inside = directory
                        .relative("static/inside.txt")
                        .expect("inside path");
                    let file = open_regular(&mut app, access, inside);
                    assert_eq!(read_all(&mut app, access, file), b"inside");

                    let outside = directory
                        .relative("static/outside-file.txt")
                        .expect("outside path");
                    fibers::TEST
                        .drive(&mut app, OpenRegular::new(access, outside))
                        .expect_err("escaping symlink must fail");
                },
            )
            .expect("application teardown");
        })
        .expect("file storage");
}
