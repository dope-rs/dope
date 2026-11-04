use std::{cell::Cell, num::NonZero};

const ERRORS: [NonZero<libc::c_int>; 2] = {
    assert!(libc::ENOMEM > 0 && libc::EAGAIN > 0);
    [
        NonZero::<u32>::MIN
            .saturating_add(libc::ENOMEM as u32 - 1)
            .cast_signed(),
        NonZero::<u32>::MIN
            .saturating_add(libc::EAGAIN as u32 - 1)
            .cast_signed(),
    ]
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Failure {
    pub address: *mut std::ffi::c_void,
    pub length: usize,
    pub code: NonZero<std::ffi::c_int>,
}

thread_local! {
    static ACTIVE: Cell<Option<[Option<Failure>; 2]>> = const { Cell::new(None) };
}

#[derive(Debug)]
pub enum Error {
    Active,
    Missing([Option<Failure>; 2]),
}

/// Records a native unmap attempt and returns its scheduled error, if any.
pub fn intercept(
    address: *mut std::ffi::c_void,
    length: usize,
) -> Option<NonZero<std::ffi::c_int>> {
    ACTIVE
        .try_with(|state| {
            let mut failures = state.get()?;
            let index = failures.iter().position(Option::is_none)?;
            let code = ERRORS[index];
            failures[index] = Some(Failure {
                address,
                length,
                code,
            });
            state.set(Some(failures));
            Some(code)
        })
        .unwrap_or(None)
}

/// Injects two ordered unmap failures on the current owning thread.
pub fn try_run(run: impl FnOnce()) -> Result<[Failure; 2], Error> {
    ACTIVE.with(|state| {
        if state.get().is_some() {
            return Err(Error::Active);
        }
        state.set(Some([None; 2]));
        run();
        let Some(failures) = state.take() else {
            unreachable!("active")
        };
        match failures {
            [Some(first), Some(second)] => Ok([first, second]),
            failures => Err(Error::Missing(failures)),
        }
    })
}
