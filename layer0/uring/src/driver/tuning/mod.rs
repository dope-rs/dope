use std::{marker, mem, num, pin};

use dope_sys::{
    io::{
        reactor::{
            Full,
            submit::{self, Queue as _, Ready as _},
        },
        socket::option::tuning,
    },
    table::token::{Codec, Context, Tuning as _},
};
use o3::{alloc, base::num::bounded};

use crate::reactor::{cq, op::controls, sq};

pub(crate) mod raw;

#[derive(Debug)]
pub struct State<E, const N: usize> {
    target: Option<E>,
    values: [libc::c_int; N],
    status: Status,
    /// Initialized by `begin`, read once after all option completions succeed.
    connect: mem::MaybeUninit<sq::raw::Deferred>,
    next: Option<sq::Fixed>,
    _pin: marker::PhantomPinned,
}

/// Progress and cancellation of the current option flight.
#[derive(Debug)]
struct Status {
    retiring: bool,
    stopped: bool,
    phase: Phase,
    /// Remaining scalar option completions, live while the option stage runs.
    count: num::NonZero<u32>,
    /// Aggregated option-stage failure, if any.
    error: Option<cq::Outcome>,
}

impl<E, const N: usize> Default for State<E, N> {
    fn default() -> Self {
        Self {
            target: None,
            values: [0; N],
            status: Status {
                retiring: false,
                stopped: false,
                phase: Phase::Idle,
                count: num::NonZero::<u32>::MIN,
                error: None,
            },
            connect: mem::MaybeUninit::uninit(),
            next: None,
            _pin: marker::PhantomPinned,
        }
    }
}

#[derive(Debug)]
pub struct Table<E, const N: usize> {
    states: pin::Pin<Box<[State<E, N>]>>,
    ready: Option<sq::Fixed>,
    tail: Option<sq::Fixed>,
}

#[derive(Clone, Copy, Debug)]
enum Phase {
    Idle,
    /// Scalar options in flight; the flight submits no deferred CONNECT.
    Options,
    /// Scalar options in flight, then one deferred CONNECT held in `connect`.
    OptionsConnect,
    /// Linked in `ready`; the deferred CONNECT waits for a submission vacancy.
    Ready,
    Connecting,
    Complete,
}

mod progress;
pub(crate) use progress::Progress;

pub struct Occupied<'a, E, const N: usize> {
    state: pin::Pin<&'a mut State<E, N>>,
    target: E,
}

pub(crate) enum Retirement<'a, E, const N: usize> {
    Ready,
    Pending,
    Cancel(self::Cancelling<'a, E, N>),
}

/// The exact active chain which must terminate before this slot can close.
pub(crate) struct Cancelling<'a, E, const N: usize> {
    state: pin::Pin<&'a mut State<E, N>>,
    slot: sq::Fixed,
}

impl<E: Copy, const N: usize> Table<E, N> {
    pub(crate) fn retirement(&mut self, slot: sq::Fixed) -> Retirement<'_, E, N> {
        use tuning::Table as _;
        let Some(state) = self.get(slot.index()) else {
            return Retirement::Ready;
        };
        if state.target.is_none() {
            return Retirement::Ready;
        }
        if state.status.retiring {
            return Retirement::Pending;
        }
        Retirement::Cancel(Cancelling { state, slot })
    }
}

impl<E, const N: usize> Cancelling<'_, E, N> {
    /// Without LINK/FORCE_ASYNC, Linux v6.12 io_submit_sqes -> io_submit_sqe -> io_queue_sqe issues io_async_cancel synchronously (IOU_OK; liburing prep_cancel.3 NOTES).
    /// It matches before a later close/new chain in that SQ; only its CQ may outlive slot reuse, where retry_cancel changes no target or owner.
    pub(crate) fn try_submit<T: Codec>(self, queue: sq::raw::Queue<'_, T>) -> Result<(), Full> {
        let vacancy = queue.try_one()?;
        let index =
            <T::Context as Context>::Slot::from(bounded::U32::clamp(self.slot.index() as usize));
        let target = T::Tuning::new(index);
        let control = cancel::<T>(target);
        submit::raw::Vacancy::prepare(vacancy, &control).push();
        // SAFETY: the published cancellation changes only the phase; the
        // pinned scalars stay alive until the tuning terminal clears them.
        let state = unsafe { self.state.get_unchecked_mut() };
        state.status.retiring = true;
        state.status.stopped = true;
        Ok(())
    }
}

impl<E, const N: usize> Table<E, N> {
    /// Projects a slot without repeating admission.
    /// # Safety
    /// `slot` indexes this exact table's immutable extent via `device::Admission` or a retained `get` proof.
    unsafe fn admitted(&mut self, slot: sq::Fixed) -> pin::Pin<&mut State<E, N>> {
        // SAFETY: the caller proves the index is in range; the boxed allocation
        // is pinned, so the projection keeps the element at its address.
        unsafe {
            self.states
                .as_mut()
                .map_unchecked_mut(|states| states.get_unchecked_mut(slot.index() as usize))
        }
    }
}

pub(in crate::driver) fn cancel<T: Codec>(target: T::Tuning) -> sq::Owned<T> {
    controls::cancel(T::tuning(target), true)
        .flags(sq::Flags::SKIP_SUCCESS)
        .user_data(T::cancel_tune(target))
}

impl<E, const N: usize> tuning::Create for Table<E, N> {
    fn try_new(capacity: num::NonZero<u32>) -> Result<Self, alloc::Error> {
        use alloc::BoxSliceExt as _;
        const {
            assert!(mem::align_of::<Self>() == mem::align_of::<pin::Pin<Box<[State<E, N>]>>>());
        };

        let states = Box::<[State<E, N>]>::try_default(capacity.get() as usize)?;
        Ok(Self {
            states: Box::into_pin(states),
            ready: None,
            tail: None,
        })
    }
}

impl<E: Copy, const N: usize> tuning::Table<E, N> for Table<E, N> {
    type State = State<E, N>;
    type Occupied<'a>
        = Occupied<'a, E, N>
    where
        Self: 'a;

    fn bind(&mut self, index: u32) -> Result<Self::Occupied<'_>, tuning::Missing> {
        let state = self.get(index).ok_or(tuning::Missing::Slot)?;
        tuning::raw::State::bind(state).ok_or(tuning::Missing::Vacant)
    }

    fn get(&mut self, index: u32) -> Option<pin::Pin<&mut Self::State>> {
        if index as usize >= self.states.len() {
            return None;
        }
        // SAFETY: the boxed allocation is pinned; the checked element cannot
        // move while this projection borrows the table exclusively.
        Some(unsafe {
            self.states
                .as_mut()
                .map_unchecked_mut(|states| states.get_unchecked_mut(index as usize))
        })
    }

    fn retry_cancel(&mut self, index: u32) {
        let Some(state) = self.get(index) else {
            return;
        };
        // SAFETY: this control reply changes only the retry marker. In
        // particular, an old reply cannot release the current pinned values or
        // target. A new cancellation still requires an owned host retirement.
        unsafe { state.get_unchecked_mut() }.status.retiring = false;
    }
}

impl<E, const N: usize> tuning::Quiesce for Table<E, N> {
    fn is_quiescent(&self) -> bool {
        self.states.iter().all(|state| state.target.is_none())
    }
}

impl<E: Copy, const N: usize> tuning::Occupied<E> for Occupied<'_, E, N> {
    fn target(&self) -> E {
        self.target
    }

    /// Only the aggregated option terminal or the later CONNECT terminal reaches
    /// the host. Every option CQE is counted even when submission splits links.
    fn clear(self) {
        // SAFETY: clearing its echo moves no pinned value and lets retirement
        // submit close; only close completion can then release this file slot.
        let state = unsafe { self.state.get_unchecked_mut() };
        state.target = None;
        state.status.retiring = false;
        state.status.stopped = false;
        state.status.phase = Phase::Idle;
    }
}

const _: () = {
    assert!(std::mem::size_of::<Table<u64, 7>>() == 3 * std::mem::size_of::<usize>());
    assert!(mem::size_of::<Status>() == 16);
    assert!(mem::size_of::<State<num::NonZero<u64>, 7>>() == 120);
};

impl<E, const N: usize> State<E, N> {
    /// # Safety
    /// No previous native operation uses this state. Retain its values and any
    /// deferred terminal address until the complete two-stage flight terminates.
    unsafe fn begin(
        self: pin::Pin<&mut Self>,
        target: E,
        count: num::NonZero<u32>,
        terminal: Option<sq::raw::Deferred>,
    ) -> &mut [libc::c_int; N] {
        // SAFETY: the caller proves no previous chain remains; only fields are
        // mutated, and the pinned array stays at its original address.
        let state = unsafe { self.get_unchecked_mut() };
        state.target = Some(target);
        state.status.count = count;
        state.status.error = None;
        state.status.phase = match terminal {
            Some(entry) => {
                state.connect.write(entry);
                Phase::OptionsConnect
            }
            None => Phase::Options,
        };
        state.status.stopped = false;
        state.status.retiring = false;
        state.next = None;
        &mut state.values
    }
}
