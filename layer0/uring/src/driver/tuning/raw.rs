use std::{mem, num, pin};

use dope_sys::{
    engine::ring::{Backend as _, Context},
    io::{
        reactor::submit::Queue as _,
        socket::{
            option,
            option::tuning::{self, device},
        },
    },
    ring::ready::Ready as _,
    table::token::{self, Codec, Tuning as _},
};
use o3::depot::capacity;

use crate::{
    Engine,
    driver::{
        Key,
        tuning::{Occupied, State, Table},
    },
    reactor::sq,
};

pub(crate) struct Vacant<'a, T: Codec, F, const N: usize> {
    state: pin::Pin<&'a mut State<Key<T>, N>>,
    fd: F,
    slot: sq::Fixed,
    token: T,
}

impl<T: Codec, F, const N: usize> Vacant<'_, T, F, N> {
    /// # Safety
    /// The cursor holds exactly `count` options for this descriptor and its admitted queue.
    /// Retain captured terminal owners and the pinned table through completion or quiescence.
    pub(crate) unsafe fn try_submit<R, E>(
        mut self,
        queue: sq::raw::Queue<'_, T>,
        count: capacity::Limit<N>,
        options: impl option::Cursor,
        last: Option<sq::Borrowed<'_>>,
        reserve: impl FnOnce() -> Result<(Key<T>, R), E>,
    ) -> Result<(F, R), tuning::Error<F, E>> {
        use tuning::raw::Chain as _;
        let ready = match queue.try_reserve(count) {
            Ok(ready) => ready,
            Err(error) => return Err(tuning::Error::Full(self.fd, error)),
        };
        let (key, target) = match reserve() {
            Ok(target) => target,
            Err(error) => return Err(tuning::Error::Reserve(self.fd, error)),
        };
        // SAFETY: the host retains the CONNECT address through the same flight;
        // it is submitted only after every option CQE has succeeded.
        let terminal = last.map(|last| unsafe { sq::raw::Deferred::new(last) });
        // SAFETY: construction retains the affine slot proof and pinned table.
        let values = unsafe { self.state.as_mut().begin(key, count.count(), terminal) };
        // SAFETY: admission bound the vacant state, target and token together;
        // the caller retains the exact cursor and terminal through completion.
        unsafe { ready.submit(values, options, self.slot, self.token) };
        Ok((self.fd, target))
    }

    /// Consumes the host's exact backend and affine descriptor admission.
    pub(crate) fn admit<'a, C>(
        admitted: device::Admission<
            'a,
            Engine<C>,
            F,
            sq::Fixed,
            <T::Context as token::Context>::Slot,
        >,
    ) -> (sq::raw::Queue<'a, T>, Vacant<'a, T, F, N>)
    where
        C: Context<Token = T, Tuning = Table<Key<T>, N>>,
        T: 'a,
    {
        let (backend, fd, slot, index) = admitted.into_parts();
        let (ring, table) = backend.split();
        // SAFETY: Admission can only be created by the unsafe host root. Its
        // affine descriptor was admitted inside this backend's immutable file
        // and tuning extent, and both targets came from that descriptor. The
        // host retains it until a previous terminal clears the pinned state,
        // so this exact entry is vacant. The exclusive backend borrow prevents
        // another projection or submission until this capability is consumed.
        let state = unsafe { table.admitted(slot) };
        let vacant = Vacant {
            state,
            fd,
            slot,
            token: T::tuning(T::Tuning::new(index)),
        };
        (ring.submission(), vacant)
    }
}

// SAFETY: projection never moves the values, and only the consumed terminal
// owner can clear the echo that marks their linked chain occupied.
unsafe impl<E: Copy, const N: usize> tuning::raw::State<E, N> for State<E, N> {
    type Occupied<'a>
        = Occupied<'a, E, N>
    where
        Self: 'a;

    fn bind(self: pin::Pin<&mut Self>) -> Option<Self::Occupied<'_>> {
        const {
            assert!(
                mem::size_of::<Occupied<'_, E, N>>() == mem::size_of::<(pin::Pin<&mut Self>, E)>()
            );
            assert!(
                mem::align_of::<Occupied<'_, E, N>>()
                    == mem::align_of::<(pin::Pin<&mut Self>, E)>()
            );
        };
        let target = self.as_ref().target?;
        Some(Occupied {
            state: self,
            target,
        })
    }

    unsafe fn values(
        self: pin::Pin<&mut Self>,
        target: E,
        count: num::NonZero<u32>,
    ) -> &mut [libc::c_int; N] {
        // SAFETY: the caller proves this exact option count and retains the
        // pinned owner until every corresponding native CQE has arrived.
        unsafe { self.begin(target, count, None) }
    }
}
