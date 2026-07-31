use std::mem::ManuallyDrop;

use super::super::OpenRollback;

pub(crate) struct Guard<'a, W, S, C>
where
    C: OpenRollback<W, S>,
{
    open: ManuallyDrop<(W, S)>,
    context: &'a mut C,
}

impl<'a, W, S, C> Guard<'a, W, S, C>
where
    C: OpenRollback<W, S>,
{
    pub(crate) fn new(context: &'a mut C, wire: W, send: S) -> Self {
        Self {
            open: ManuallyDrop::new((wire, send)),
            context,
        }
    }

    pub(crate) fn commit(self) -> (W, S) {
        let mut this = ManuallyDrop::new(self);
        // SAFETY: consuming self prevents Drop from taking open a second time.
        unsafe { ManuallyDrop::take(&mut this.open) }
    }
}

impl<W, S, C> Drop for Guard<'_, W, S, C>
where
    C: OpenRollback<W, S>,
{
    fn drop(&mut self) {
        // SAFETY: open is initialized by new and Drop runs only without commit.
        let open = unsafe { ManuallyDrop::take(&mut self.open) };
        self.context.rollback_open(open);
    }
}
