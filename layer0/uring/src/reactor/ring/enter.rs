use std::{marker, mem, ptr, time};

use dope_sys::{
    kernel,
    ring::{enter, prepare::Feature},
};
use o3::base::num::bounded;

use crate::{
    Ring, error,
    reactor::{ring, sq},
};

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(C)]
/// A validated relative wait duration for io_uring.
pub struct Timeout {
    seconds: bounded::U64<0, { i64::MAX as u64 }>,
    nanoseconds: bounded::U64<0, 999_999_999>,
}

impl Timeout {
    fn into_raw(self) -> ring::ffi::Timespec {
        ring::ffi::Timespec {
            seconds: self.seconds.get() as i64,
            nanoseconds: self.nanoseconds.get() as i64,
        }
    }
}

#[must_use = "an enter snapshot must be submitted, waited, or committed exactly once"]
pub struct Enter<'a, T> {
    owner: marker::PhantomData<&'a mut Ring<T>>,
    binding: ring::ffi::Binding<'a>,
    state: State,
}

struct State {
    submit: sq::Pending,
    sq_flags: u32,
}

#[repr(C)]
struct Arguments {
    signal_mask: u64,
    signal_mask_size: u32,
    minimum_wait_microseconds: u32,
    timespec: u64,
}

impl TryFrom<time::Duration> for Timeout {
    type Error = enter::Overflow;

    fn try_from(value: time::Duration) -> Result<Self, Self::Error> {
        let Some(seconds) = bounded::U64::new(value.as_secs()) else {
            return Err(enter::Overflow::new(value));
        };
        let nanos = bounded::U64::<0, 999_999_999>::clamp(u64::from(value.subsec_nanos()));
        Ok(Self {
            seconds,
            nanoseconds: nanos,
        })
    }
}

impl<'a, T> Enter<'a, T> {
    pub(crate) fn new(queue: &'a mut sq::raw::State, binding: ring::ffi::Binding<'a>) -> Self {
        queue.publish();
        let submit = queue.pending();
        let sq_flags = queue.flags();
        Self {
            owner: marker::PhantomData,
            binding,
            state: State { submit, sq_flags },
        }
    }
}

impl<T> enter::Enter for Enter<'_, T> {
    type Error = kernel::Errno<error::Enter>;
    type Timeout = Timeout;

    fn try_submit(self) -> Result<(), Self::Error> {
        self.state.try_submit(self.binding)
    }

    fn try_wait(self, timeout: Option<Self::Timeout>) -> Result<(), Self::Error> {
        self.state.try_wait(self.binding, timeout)
    }

    fn try_commit(self) -> Result<enter::Progress, Self::Error> {
        self.state.try_commit(self.binding)
    }
}

impl State {
    fn try_submit(
        self,
        binding: ring::ffi::Binding<'_>,
    ) -> Result<(), kernel::Errno<error::Enter>> {
        self.try_enter(binding, 0, 0, ptr::null(), mem::size_of::<libc::sigset_t>())
    }

    fn try_wait(
        self,
        binding: ring::ffi::Binding<'_>,
        timeout: Option<Timeout>,
    ) -> Result<(), kernel::Errno<error::Enter>> {
        use crate::reactor::ring::ffi;

        match timeout {
            Some(timeout) => {
                let timespec = timeout.into_raw();
                let args = Arguments {
                    signal_mask: 0,
                    signal_mask_size: 0,
                    minimum_wait_microseconds: 0,
                    timespec: ptr::from_ref(&timespec) as u64,
                };
                self.try_enter(
                    binding,
                    1,
                    ffi::ENTER_EXT_ARG,
                    ptr::from_ref(&args).cast(),
                    mem::size_of::<Arguments>(),
                )
            }
            None => self.try_enter(binding, 1, 0, ptr::null(), mem::size_of::<libc::sigset_t>()),
        }
    }

    fn try_commit(
        self,
        binding: ring::ffi::Binding<'_>,
    ) -> Result<enter::Progress, kernel::Errno<error::Enter>> {
        use crate::reactor::ring::ffi;

        let progress = self.sq_flags & (ffi::SQ_TASKRUN | ffi::SQ_CQ_OVERFLOW) != 0;
        if self.submit.is_zero() && !progress {
            return Ok(enter::Progress::Idle);
        }
        let flags = if progress { ffi::ENTER_GETEVENTS } else { 0 };
        self.try_enter(
            binding,
            0,
            flags,
            ptr::null(),
            mem::size_of::<libc::sigset_t>(),
        )?;
        Ok(enter::Progress::Entered)
    }

    const fn has_overflow(&self) -> bool {
        use crate::reactor::ring::ffi;

        self.sq_flags & ffi::SQ_CQ_OVERFLOW != 0
    }

    fn try_enter(
        self,
        binding: ring::ffi::Binding<'_>,
        complete: u32,
        mut flags: u32,
        args: *const libc::c_void,
        size: usize,
    ) -> Result<(), kernel::Errno<error::Enter>> {
        use crate::reactor::ring::ffi;

        if complete != 0 || self.has_overflow() {
            flags |= ffi::ENTER_GETEVENTS;
        }
        ffi::Fd::try_enter(binding, self.submit, complete, flags, args, size)
    }
}

const _: () = {
    assert!(mem::size_of::<Timeout>() == 16);
    assert!(mem::align_of::<Timeout>() == 8);
    assert!(mem::size_of::<ring::ffi::Timespec>() == mem::size_of::<Timeout>());
    assert!(mem::align_of::<ring::ffi::Timespec>() == mem::align_of::<Timeout>());
    assert!(mem::size_of::<Arguments>() == 24);
    assert!(mem::size_of::<State>() == 8);
    assert!(mem::size_of::<Enter<'static, ()>>() == 16);
};

#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub(crate) struct Features(u32);

impl Features {
    pub(crate) const fn new(bits: u32) -> Self {
        Self(bits)
    }

    pub(crate) const fn has_ext_arg(self) -> bool {
        use crate::reactor::ring::ffi;

        self.0 & ffi::FEATURE_EXT_ARG != 0
    }

    pub(crate) const fn has_no_drop(self) -> bool {
        use crate::reactor::ring::ffi;

        self.0 & ffi::FEATURE_NODROP != 0
    }

    pub(crate) const fn has_cqe_skip(self) -> bool {
        use crate::reactor::ring::ffi;

        self.0 & ffi::FEATURE_CQE_SKIP != 0
    }

    pub(super) fn require(self) -> Result<(), Feature> {
        if !self.has_ext_arg() {
            return Err(Feature::ExtendedArguments);
        }
        if !self.has_no_drop() {
            return Err(Feature::NoDrop);
        }
        if !self.has_cqe_skip() {
            return Err(Feature::SkipCompletionOnSuccess);
        }
        Ok(())
    }
}
