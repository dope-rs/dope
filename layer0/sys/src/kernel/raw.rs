use std::num;

use o3::base::num::bounded;

use crate::{Errno, kernel};

/// Access to errno immediately after a native operation reports failure.
/// # Safety
/// `read` returns the unchanged native errno slot of the calling thread.
pub unsafe trait Source {
    /// Reads this thread's unchanged native errno slot.
    fn read() -> i32;
}

/// A positive errno established by one native failure or completion field.
#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct Code(num::NonZero<i32>);

impl Code {
    /// Retains the positive error number established by one failed native call.
    ///
    /// # Safety
    /// A failed native call left this thread's errno positive and unchanged.
    pub unsafe fn last<S: Source + ?Sized>() -> Self {
        // SAFETY: the caller retains the immediately preceding native failure's positive errno.
        unsafe { Self::new(S::read()) }
    }

    /// Retains the positive domain established by the native producer.
    ///
    /// # Safety
    /// `raw` is positive, as established by the unchanged native error field.
    pub const unsafe fn new(raw: i32) -> Self {
        // SAFETY: the caller retains the native producer's positive errno proof.
        Self(unsafe { num::NonZero::new_unchecked(raw) })
    }

    /// Retains the errno one native completion field reports, if any.
    ///
    /// # Safety
    /// `raw` is that completion's exact field: zero or a positive c_int.
    pub unsafe fn from_native(raw: libc::intptr_t) -> Option<Self> {
        num::NonZero::new(raw as i32).map(Self)
    }
}

impl From<Code> for Errno {
    fn from(code: Code) -> Self {
        Self(code.0)
    }
}

/// A native scalar retained with the exact domain its producer guarantees, so
/// every decoder below is total: the producer's ABI proof is discharged once at
/// construction and reused without a second range check.
#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct Status<const MIN: libc::c_int, const MAX: libc::c_int>(libc::c_int);

impl<const MIN: libc::c_int, const MAX: libc::c_int> Status<MIN, MAX> {
    /// Retains the return domain established by one native operation.
    ///
    /// # Safety
    /// `raw` lies in `MIN..=MAX`, as guaranteed by the operation producing it.
    pub const unsafe fn new(raw: libc::c_int) -> Self {
        Self(raw)
    }
}

/// Retries only interrupted native attempts, preserving each leaf's success domain.
pub(crate) fn try_retry<T, E, const RETRY: bool>(
    mut call: impl FnMut() -> Result<T, Errno>,
    classify: impl FnOnce(Errno) -> E,
) -> Result<T, E> {
    loop {
        match call() {
            Ok(value) => return Ok(value),
            Err(error) if RETRY && error.is_intr() => continue,
            Err(error) => return Err(classify(error)),
        }
    }
}

impl Status<-1, 0> {
    /// Decodes a native zero-or-minus-one operation without erasing errno.
    ///
    /// # Safety
    /// `errno` reads that thread's slot before another syscall.
    pub unsafe fn try_zero<E: From<Errno>>(self, errno: impl FnOnce() -> Errno) -> Result<(), E> {
        // SAFETY: -1..=0 is Exact<0>'s domain; errno keeps the caller's contract.
        unsafe { Exact::<0>(self.0).try_admit(errno) }
    }
}

impl Status<-1, { i32::MAX }> {
    /// Retains a native count whose exact syscall operand already bounds it.
    ///
    /// # Safety
    /// `raw` is the unchanged return of that count-returning syscall: native -1 or `0..=i32::MAX`.
    pub const unsafe fn from_count(raw: libc::ssize_t) -> Self {
        Self(raw as libc::c_int)
    }

    /// Converts the retained native scalar domain without checking its range again.
    pub fn try_value<E: From<Errno>>(
        self,
        errno: impl FnOnce() -> Errno,
    ) -> Result<kernel::Value, E> {
        if self.0 == -1 {
            Err(E::from(errno()))
        } else {
            Ok(kernel::Value(self.0))
        }
    }

    /// Decodes one count-returning operation against the exact extent it was given.
    ///
    /// # Safety
    /// `errno` reads that thread's slot before another syscall.
    pub unsafe fn try_count<E: From<Errno>>(
        self,
        errno: impl FnOnce() -> Errno,
    ) -> Result<Count, E> {
        Ok(Count::new(self.try_value(errno)?))
    }

    /// Tightens this scalar to one operation's exact `-1`-or-`VALUE` return.
    ///
    /// # Safety
    /// That operation reports exactly `VALUE` or -1 for these arguments.
    pub const unsafe fn admit_exact<const VALUE: libc::c_int>(self) -> Exact<VALUE> {
        Exact(self.0)
    }
}

/// One native return whose producer reports exactly -1 or its one success value,
/// so the ABI's exactness proof is discharged once at admission and the decoder
/// below is total without a second range check.
#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct Exact<const VALUE: libc::c_int>(libc::c_int);

impl<const VALUE: libc::c_int> Exact<VALUE> {
    /// Decodes the admitted success value without re-deriving its domain.
    ///
    /// # Safety
    /// `errno` reads that thread's slot before another syscall.
    pub unsafe fn try_admit<E: From<Errno>>(self, errno: impl FnOnce() -> Errno) -> Result<(), E> {
        if self.0 == VALUE {
            Ok(())
        } else {
            Err(E::from(errno()))
        }
    }
}

impl Status<0, { i32::MAX }> {
    /// Decodes the error code returned directly by a pthread operation.
    pub fn try_pthread(self) -> Result<(), Errno> {
        if self.0 == 0 {
            Ok(())
        } else {
            // SAFETY: pthread's retained return domain makes every failure positive.
            Err(unsafe { Code::new(self.0) }.into())
        }
    }
}

/// One live descriptor whose native producer or retained owner establishes its
/// nonnegative domain, so later uses do not check that range again.
#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct Descriptor(bounded::U32<0, { i32::MAX as u32 }>);

impl Descriptor {
    /// # Safety
    /// `raw` is the unchanged live nonnegative descriptor produced by a successful
    /// native operation or retained by its exclusive owner.
    pub const unsafe fn new(raw: libc::c_int) -> Self {
        // SAFETY: the native nonnegative c_int retains the exact 0..=i32::MAX bound.
        Self(unsafe { bounded::raw::Proof::assume(raw as u32) }.retain())
    }

    pub const fn get(self) -> libc::c_int {
        self.0.get() as libc::c_int
    }
}

/// One completed native count retaining its admitted ABI bound.
#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct Count(bounded::U32<0, { i32::MAX as u32 }>);

impl Count {
    /// Retains the count one count-returning operation completed within its operand.
    const fn new(value: kernel::Value) -> Self {
        // SAFETY: Value admits exactly 0..=i32::MAX, the bound retained here.
        Self(unsafe { bounded::raw::Proof::assume(value.get() as u32) }.retain())
    }

    pub const fn get(self) -> usize {
        self.0.into_usize()
    }

    pub const fn bounded(self) -> bounded::U32<0, { i32::MAX as u32 }> {
        self.0
    }
}
