use std::{cell::Cell, num::NonZero};

use o3::base::marker::ThreadBound;

pub const ERRORS: [NonZero<libc::c_int>; 2] = {
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

#[derive(Clone, Copy)]
struct State {
    codes: [NonZero<std::ffi::c_int>; 2],
    failures: [Option<Failure>; 2],
}

thread_local! {
    static ACTIVE: Cell<Option<State>> = const { Cell::new(None) };
}

#[derive(Debug)]
pub enum Error {
    Active,
    Inactive,
    Missing([Option<Failure>; 2]),
}

/// Injects two ordered unmap failures on the current owning thread.
pub struct Schedule([NonZero<std::ffi::c_int>; 2]);

struct Active<'a>(&'a Cell<Option<State>>, ThreadBound);

impl Active<'_> {
    fn take(self) -> Result<State, Error> {
        self.0.take().ok_or(Error::Inactive)
    }
}

impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.set(None);
    }
}

impl Schedule {
    /// Records a native unmap attempt and returns its scheduled error, if any.
    pub fn intercept(
        address: *mut std::ffi::c_void,
        length: usize,
    ) -> Option<NonZero<std::ffi::c_int>> {
        ACTIVE
            .try_with(|state| {
                let mut schedule = state.get()?;
                let index = schedule.failures.iter().position(Option::is_none)?;
                let code = schedule.codes[index];
                schedule.failures[index] = Some(Failure {
                    address,
                    length,
                    code,
                });
                state.set(Some(schedule));
                Some(code)
            })
            .unwrap_or(None)
    }

    pub const fn new(codes: [NonZero<std::ffi::c_int>; 2]) -> Self {
        Self(codes)
    }

    pub fn try_run(self, run: impl FnOnce()) -> Result<[Failure; 2], Error> {
        ACTIVE.with(|state| {
            if state.get().is_some() {
                return Err(Error::Active);
            }
            state.set(Some(State {
                codes: self.0,
                failures: [None; 2],
            }));
            let active = Active(state, ThreadBound::NEW);
            run();
            let schedule = active.take()?;
            match schedule.failures {
                [Some(first), Some(second)] => Ok([first, second]),
                failures => Err(Error::Missing(failures)),
            }
        })
    }
}
