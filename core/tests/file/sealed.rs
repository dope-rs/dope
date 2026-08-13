use std::{mem, ops, os::unix::fs::MetadataExt, path::Path, pin::Pin};

use dope_core::{driver, io::fs};

pub(super) fn dispatch_all<'d>(
    driver: &mut driver::Context<'_, 'd>,
    work: driver::schedule::Reactor<'_, 'd>,
    mut dispatch: impl FnMut(dope_core::io::Event<'d>),
) -> driver::ops::poll::Drain {
    let dispatched = driver::ops::poll::Source::dispatch(driver, work, |event, _driver| {
        dispatch(event);
        ops::ControlFlow::Continue(())
    });
    let (drain, retained) = dispatched.into_parts();
    assert!(retained.is_none());
    drain
}

pub(super) fn submit_retained<'owner, 'd: 'owner, Tag: driver::route::Tag>(
    driver: &mut driver::retained::Context<'_, 'owner, 'd>,
    submission: fs::Submission<'_, 'd, fs::Native, Tag>,
) -> Result<(), driver::SubmitError> {
    let slots = driver
        .flight_slots::<Tag>(1)
        .map_err(|_| driver::SubmitError)?;
    // SAFETY: every test observes the matching completion or leaves the scope
    // through final synchronous quiescence before touching backing resources.
    unsafe { driver::retained::raw::Owner::submit_file(driver, &slots, submission) }.map(drop)
}

pub(super) fn open_fds_for(path: &Path) -> Vec<libc::c_int> {
    let metadata = std::fs::metadata(path).expect("tracked file metadata");
    (0..4096)
        .filter(|fd| {
            let mut stat = mem::MaybeUninit::<libc::stat>::uninit();
            // SAFETY: stat points to writable storage and fstat initializes it on success.
            if unsafe { libc::fstat(*fd, stat.as_mut_ptr()) } != 0 {
                return false;
            }
            // SAFETY: successful fstat initialized the complete value.
            let stat = unsafe { stat.assume_init() };
            stat.st_dev as u128 == u128::from(metadata.dev())
                && stat.st_ino as u128 == u128::from(metadata.ino())
        })
        .collect()
}

pub(super) fn retained_context<'a, 'owner, 'd: 'owner, T: ?Sized>(
    context: driver::Context<'a, 'd>,
    owner: Pin<&'owner T>,
) -> driver::retained::Context<'a, 'owner, 'd> {
    // SAFETY: the caller's pin borrow remains live through explicit final
    // quiescence, which settles every request retaining owner-backed storage.
    let owner = unsafe { driver::retained::raw::Owner::new(owner) };
    driver::retained::Context::new(context, owner)
}
