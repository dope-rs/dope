use std::{mem, ptr};

use dope_sys::{Errno, kernel, thread::timer};
use libc::time_t;
use o3::base::time::Positive;

use crate::Uring;

/// One `timer_settime` deadline operand the kernel admits for a live identifier.
#[derive(Clone, Copy)]
#[repr(transparent)]
struct Deadline(libc::timespec);

impl Deadline {
    /// The zero deadline: the interval of every one-shot arming and the value
    /// that disarms a live identifier.
    const ZERO: Self = Self(libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    });

    /// Admits one deadline, retaining the exact out-of-range rejection.
    fn try_new(deadline: Positive) -> Result<Self, timer::Error> {
        let duration = deadline.get();
        let seconds = duration.as_secs();
        if u128::from(seconds) > time_t::MAX as u128 {
            return Err(timer::Error::Range { deadline });
        }
        Ok(Self(libc::timespec {
            tv_sec: seconds as time_t,
            tv_nsec: duration.subsec_nanos().into(),
        }))
    }
}

pub struct Timer {
    timer: libc::timer_t,
    value: Deadline,
}

impl dope_sys::Timer for Timer {
    fn try_new(deadline: Positive) -> Result<Self, timer::Error> {
        let value = Deadline::try_new(deadline)?;
        let mut event = unsafe { mem::zeroed::<libc::sigevent>() };
        event.sigev_notify = libc::SIGEV_SIGNAL;
        event.sigev_signo = libc::SIGKILL;
        let mut timer = mem::MaybeUninit::<libc::timer_t>::uninit();
        // SAFETY: event is initialized for process-directed SIGKILL delivery
        // and timer is writable for one timer identifier.
        let result =
            unsafe { libc::timer_create(libc::CLOCK_MONOTONIC, &mut event, timer.as_mut_ptr()) };
        // SAFETY: timer_create returns zero or -1 with errno.
        unsafe {
            kernel::raw::Status::new(result)
                .try_zero::<Errno>(|| kernel::raw::Code::last::<Uring>().into())
        }
        .map_err(timer::Error::Create)?;
        // SAFETY: successful timer_create initialized the identifier.
        let timer = Self {
            timer: unsafe { timer.assume_init() },
            value,
        };
        timer.try_set(value).map_err(timer::Error::Arm)?;
        Ok(timer)
    }

    fn try_refresh(&self) -> Result<(), Errno> {
        self.try_set(self.value)
    }

    fn try_disarm(&self) -> Result<(), Errno> {
        self.try_set(Deadline::ZERO)
    }
}

impl Timer {
    /// Arms or clears this live timer's deadline with one admitted operand.
    /// The owned identifier and nonnegative `Deadline` with `tv_nsec < 1e9`
    /// exclude `timer_settime`'s invalid identifier and operand failures.
    fn try_set(&self, value: Deadline) -> Result<(), Errno> {
        let setting = libc::itimerspec {
            it_interval: Deadline::ZERO.0,
            it_value: value.0,
        };
        // SAFETY: this owner retains the live identifier and initialized setting;
        // timer_settime returns zero or -1 with errno.
        unsafe {
            kernel::raw::Status::new(libc::timer_settime(
                self.timer,
                0,
                &setting,
                ptr::null_mut(),
            ))
            .try_zero::<Errno>(|| kernel::raw::Code::last::<Uring>().into())
        }
    }
}

impl Drop for Timer {
    /// `EINVAL` requires an identifier this owner never replaced or deleted.
    fn drop(&mut self) {
        // SAFETY: self exclusively owns one live identifier and timer_delete
        // returns zero or -1 with errno.
        if let Err(cause) = unsafe {
            kernel::raw::Status::new(libc::timer_delete(self.timer))
                .try_zero::<Errno>(|| kernel::raw::Code::last::<Uring>().into())
        } {
            kernel::Fatal::TimerDelete(cause).terminate();
        }
    }
}

const _: () = {
    assert!(
        mem::size_of::<Timer>()
            == mem::size_of::<libc::timer_t>() + mem::size_of::<libc::timespec>()
    );
    assert!(
        mem::size_of::<Result<Timer, timer::Error>>()
            == mem::size_of::<Timer>() + mem::size_of::<usize>()
    );
};
