use core::num::NonZero;

use o3::depot::array;

pub(super) struct State<const N: usize> {
    len: array::Len<N>,
    next: array::Len<N>,
    left: Option<NonZero<u32>>,
}

pub(super) struct Push<'a, const N: usize> {
    state: &'a mut State<N>,
    slot: array::Vacancy<N>,
}

pub(super) struct Fresh<'a, const N: usize> {
    state: &'a mut State<N>,
    slot: array::Vacancy<N>,
    left: NonZero<u32>,
}

pub(super) struct Pending<'a, const N: usize> {
    state: &'a mut State<N>,
    left: NonZero<u32>,
}

pub(super) enum Completion {
    Pending,
    Ready,
}

pub(super) enum Status<const N: usize> {
    Live,
    Done(Completed<N>),
}

pub(super) enum PendingState<'a, const N: usize> {
    Live(Pending<'a, N>),
    Done(Completed<N>),
}

pub(in crate::task) struct Completed<const N: usize>(array::Len<N>);

impl<const N: usize> State<N> {
    pub(super) fn new(len: array::Len<N>) -> Self {
        Self {
            len,
            next: array::Len::ZERO,
            left: NonZero::<u32>::new(len.get() as u32),
        }
    }

    pub(super) const fn next(&self) -> array::Len<N> {
        self.next
    }

    pub(super) const fn completed(&self) -> Option<Completed<N>> {
        if self.left.is_some() {
            return None;
        }
        Some(Completed(self.len))
    }

    pub(super) fn has_fresh(&self) -> bool {
        self.next < self.len
    }

    pub(super) fn vacancy(&mut self) -> Option<Push<'_, N>> {
        if !self.next.is_empty() {
            return None;
        }
        let slot = self.len.vacancy()?;
        Some(Push { state: self, slot })
    }

    pub(super) fn fresh(&mut self) -> Option<Fresh<'_, N>> {
        let slot = self
            .next
            .vacancy()
            .filter(|slot| slot.index() < self.len.get())?;
        let left = self.left?;
        Some(Fresh {
            state: self,
            slot,
            left,
        })
    }

    pub(super) fn pending(&mut self) -> PendingState<'_, N> {
        let Some(left) = self.left else {
            return PendingState::Done(Completed(self.len));
        };
        PendingState::Live(Pending { state: self, left })
    }

    pub(super) fn reset(&mut self) {
        self.len = array::Len::ZERO;
        self.next = array::Len::ZERO;
        self.left = None;
    }
}

impl<const N: usize> Push<'_, N> {
    pub(super) const fn slot(&self) -> &array::Vacancy<N> {
        &self.slot
    }

    pub(super) fn commit(self) {
        let len = self.slot.commit();
        // SAFETY: consuming a vacancy produces an admitted length in 1..=N,
        // and Len<N> has a u32 representation.
        let left = unsafe { NonZero::<u32>::new_unchecked(len.get() as u32) };
        self.state.len = len;
        self.state.left = Some(left);
    }
}

impl<const N: usize> Fresh<'_, N> {
    pub(super) const fn slot(&self) -> &array::Vacancy<N> {
        &self.slot
    }

    pub(super) fn commit(self, done: Completion) -> Status<N> {
        self.state.next = self.slot.commit();
        Pending {
            state: self.state,
            left: self.left,
        }
        .commit(done)
    }
}

impl<const N: usize> Pending<'_, N> {
    pub(super) fn commit(self, done: Completion) -> Status<N> {
        self.state.left = match done {
            Completion::Pending => Some(self.left),
            Completion::Ready => NonZero::<u32>::new(self.left.get() - 1),
        };
        match self.state.left {
            Some(_) => Status::Live,
            None => Status::Done(Completed(self.state.len)),
        }
    }
}

impl<const N: usize> Completed<N> {
    pub(in crate::task) const fn len(&self) -> array::Len<N> {
        self.0
    }
}
