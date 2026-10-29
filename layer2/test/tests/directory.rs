use std::{
    cell::Cell,
    env, fs, io,
    os::unix::fs::{PermissionsExt as _, symlink},
};

use dope_core::Errno;
use dope_test::harness::support::file::{Directory, Remove};

#[test]
fn preserves_targets() {
    let base = env::temp_dir();
    let outside =
        Directory::try_with(&base, "directory_target", cleanup).expect("external fixture");
    let retained = outside.path().join("retained");
    fs::write(&retained, b"retained").expect("external file");
    let root = Directory::try_with(&base, "directory_root", cleanup).expect("removed fixture");
    let child = root.path().join("child");
    fs::create_dir(&child).expect("nested directory");
    fs::write(child.join("data"), b"removed").expect("nested file");
    symlink(outside.path(), child.join("link")).expect("child symlink");

    root.try_remove().expect("recursive removal");
    assert!(!root.path().try_exists().expect("removed root"));
    assert_eq!(
        fs::read(&retained).expect("retained external file"),
        b"retained"
    );

    symlink(outside.path(), root.path()).expect("root symlink");
    root.try_remove().expect("unlink root symlink");
    assert_eq!(
        fs::read(&retained).expect("retained root target"),
        b"retained"
    );

    fs::write(root.path(), b"regular").expect("root replaced by regular file");
    assert!(matches!(
        root.try_remove(),
        Err(Remove::Io(error)) if error == Errno::NOT_DIRECTORY
    ));
    assert_eq!(
        fs::read(root.path()).expect("regular root remains"),
        b"regular"
    );
    fs::remove_file(root.path()).expect("regular root cleanup");
    assert!(matches!(
        root.try_remove(),
        Err(Remove::Io(error)) if error == Errno::NOT_FOUND
    ));
}

#[test]
fn unreadable_parent() {
    let base = env::temp_dir();
    let parent = Directory::try_with(&base, "directory_parent", cleanup).expect("parent fixture");
    fs::set_permissions(parent.path(), fs::Permissions::from_mode(0o300))
        .expect("write and search without directory read");
    let child = Directory::try_with(parent.path(), "child", cleanup).expect("known child creation");
    let removed = child.try_remove();
    fs::set_permissions(parent.path(), fs::Permissions::from_mode(0o700))
        .expect("restore parent cleanup permissions");
    removed.expect("removal needs no parent directory read");
    assert!(!child.path().try_exists().expect("removed child"));
}

#[test]
fn cleans_failed_scope() {
    let base = env::temp_dir();
    let calls = Cell::new(0);
    let root = Directory::try_with(&base, "directory_scope", |result| {
        cleanup(result);
        calls.set(calls.get() + 1);
    })
    .expect("scope fixture");
    let path = root.path().to_owned();
    let result = (|| -> io::Result<()> {
        let root = root;
        fs::read(root.path().join("missing"))?;
        Ok(())
    })();
    assert_eq!(
        result.expect_err("missing input").kind(),
        io::ErrorKind::NotFound
    );
    assert_eq!(calls.get(), 1);
    assert!(!path.try_exists().expect("scope cleanup"));

    let root = Directory::try_with(&base, "directory_close", |result| {
        cleanup(result);
        calls.set(calls.get() + 1);
    })
    .expect("explicit fixture");
    let path = root.path().to_owned();
    root.try_close().expect("explicit cleanup");
    assert_eq!(calls.get(), 1);
    assert!(!path.try_exists().expect("explicit removal"));

    let mut removed = None;
    {
        let root = Directory::try_with(&base, "directory_missing", |result| removed = Some(result))
            .expect("missing fixture");
        fs::remove_dir(root.path()).expect("remove fixture before its owner");
    }
    assert!(matches!(
        removed,
        Some(Err(Remove::Io(error))) if error == Errno::NOT_FOUND
    ));
}

#[test]
fn keeps_cleanup_owner() {
    let base = env::temp_dir();
    let parent = Directory::try_with(&base, "directory_owner", cleanup).expect("parent fixture");
    let path = parent.path().to_owned();
    let calls = Cell::new(0);
    let seen = &calls;
    let child = Directory::try_with(&path, "child", move |result| {
        cleanup(result);
        seen.set(seen.get() + 1);
        drop(parent);
    })
    .expect("child fixture");
    child.try_remove().expect("replace child directory");
    fs::write(child.path(), b"regular").expect("regular child");
    assert!(matches!(
        child.try_close(),
        Err(Remove::Io(error)) if error == Errno::NOT_DIRECTORY
    ));
    assert_eq!(calls.get(), 0);
    assert!(!path.try_exists().expect("observer owner cleanup"));
}

fn cleanup(result: Result<(), Remove>) {
    match result {
        Err(Remove::Io(error)) if error == Errno::NOT_FOUND => {}
        result => result.expect("fixture cleanup"),
    }
}
