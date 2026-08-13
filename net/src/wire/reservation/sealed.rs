use std::mem;

use crate::wire::{self, contract};

#[repr(transparent)]
pub struct ReservedOpen<'a, W, S, C>(Guard<'a, W, S, C>)
where
    C: wire::OpenRollback<W, S>;

impl<'a, W, S, C> ReservedOpen<'a, W, S, C>
where
    C: wire::OpenRollback<W, S>,
{
    pub fn new(context: &'a mut C, wire: W, send: S) -> Self {
        Self(Guard::new(context, wire, send))
    }
}

impl<W, S, C> wire::OpenReservation<W, S> for ReservedOpen<'_, W, S, C>
where
    C: wire::OpenRollback<W, S>,
{
    fn commit(self) -> (W, S) {
        let Self(guard) = self;
        guard.commit()
    }
}

impl<W, S, C> contract::Contract for ReservedOpen<'_, W, S, C> where C: wire::OpenRollback<W, S> {}

struct Guard<'a, W, S, C>
where
    C: wire::OpenRollback<W, S>,
{
    open: mem::ManuallyDrop<(W, S)>,
    context: &'a mut C,
}

impl<'a, W, S, C> Guard<'a, W, S, C>
where
    C: wire::OpenRollback<W, S>,
{
    fn new(context: &'a mut C, wire: W, send: S) -> Self {
        Self {
            open: mem::ManuallyDrop::new((wire, send)),
            context,
        }
    }

    fn commit(self) -> (W, S) {
        let mut this = mem::ManuallyDrop::new(self);
        // SAFETY: consuming self prevents Drop from taking open a second time.
        unsafe { mem::ManuallyDrop::take(&mut this.open) }
    }
}

impl<W, S, C> Drop for Guard<'_, W, S, C>
where
    C: wire::OpenRollback<W, S>,
{
    fn drop(&mut self) {
        // SAFETY: open is initialized by new and Drop runs only without commit.
        let open = unsafe { mem::ManuallyDrop::take(&mut self.open) };
        <C as wire::OpenRollback<W, S>>::rollback_open(self.context, open);
    }
}
