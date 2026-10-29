use crate::probe::{Cause, Outcome, State, Stopped};

type Fault<W, S> = Cause<
    <W as Wait<S>>::Probe,
    <<W as Wait<S>>::Outcome as Outcome>::Observation,
    <W as Wait<S>>::Error,
>;

type Progress<W, S> = State<S, W>;
type Transition<T, S, W> = Result<T, Stopped<S, Fault<W, S>>>;

type Advanced<W, S> =
    Result<(Progress<W, S>, <W as Wait<S>>::Outcome), Stopped<Progress<W, S>, Fault<W, S>>>;
type Cancelled<W, S> = Result<S, Stopped<Progress<W, S>, Fault<W, S>>>;

/// A submitted capability canary retaining its complete owner until completion.
pub trait Wait<S>: Sized {
    type Ring;
    type Owned;
    type Target;
    type Probe: Copy;
    type Outcome: Outcome<Probe = Self::Probe>;
    type Error;

    fn submit(
        state: S,
        ring: impl for<'a> FnOnce(&'a mut S) -> &'a mut Self::Ring,
        entry: &Self::Owned,
        probe: Self::Probe,
    ) -> Result<Self, Stopped<S, Fault<Self, S>>>;

    /// Reports the retained cause, then acknowledges only its unchanged unread head.
    /// Already-consumed completion checks never advance the queue during recovery.
    fn resume(
        stopped: Stopped<Self, Fault<Self, S>>,
        ring: impl for<'a> FnOnce(&'a mut S) -> &'a mut Self::Ring,
        report: impl FnMut(Fault<Self, S>),
    ) -> Result<Self, Stopped<Self, Fault<Self, S>>>;

    fn finish(
        self,
        ring: impl for<'a> Fn(&'a mut S) -> &'a mut Self::Ring,
    ) -> Transition<(S, Self::Outcome), S, Self>;

    fn next(self, ring: impl for<'a> FnOnce(&'a mut S) -> &'a mut Self::Ring) -> Advanced<Self, S>;

    fn cancel(self, ring: impl for<'a> Fn(&'a mut S) -> &'a mut Self::Ring) -> Cancelled<Self, S>;

    /// # Safety
    /// The scalar stays at the same live address within state until terminal completion.
    unsafe fn option(
        state: S,
        access: impl for<'a> FnOnce(&'a mut S) -> (&'a mut Self::Ring, Self::Target, &'a libc::c_int),
        probe: Self::Probe,
    ) -> Result<Self, Stopped<S, Fault<Self, S>>>;

    /// # Safety
    /// The inspector must retain the same owner and every outstanding kernel borrow.
    unsafe fn inspect<E>(
        self,
        check: impl FnOnce(&mut S) -> Result<(), E>,
    ) -> Result<Self, Stopped<Self, E>>;
}
