use std::cell;

use dope_sys::{Errno, kernel, kernel::raw::Status};

#[test]
fn zero_preserves_errno() {
    for (raw, expected) in [
        (0, Ok(())),
        (-1, Err(kernel::Errno::from(Errno::BAD_DESCRIPTOR))),
    ] {
        let calls = cell::Cell::new(0);
        // SAFETY: the fixture contains only the native zero-or-minus-one domain.
        let result = unsafe {
            Status::<-1, 0>::new(raw).try_zero::<kernel::Errno<()>>(|| {
                calls.set(calls.get() + 1);
                Errno::BAD_DESCRIPTOR
            })
        };
        assert_eq!(result, expected);
        assert_eq!(calls.get(), usize::from(raw == -1));
    }
}

#[test]
fn count_preserves_errno() {
    for (raw, expected) in [
        (7, Ok(7)),
        (-1, Err(kernel::Errno::from(Errno::BAD_DESCRIPTOR))),
    ] {
        let calls = cell::Cell::new(0);
        // SAFETY: the fixture contains only the native count-or-minus-one domain.
        let result = unsafe {
            Status::<-1, { i32::MAX }>::new(raw).try_count::<kernel::Errno<()>>(|| {
                calls.set(calls.get() + 1);
                Errno::BAD_DESCRIPTOR
            })
        };
        assert_eq!(result.map(kernel::raw::Count::get), expected);
        assert_eq!(calls.get(), usize::from(raw == -1));
    }
}
