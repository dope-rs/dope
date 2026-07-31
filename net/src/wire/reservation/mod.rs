mod raw;

use self::raw::guard::Guard;
use super::{OpenReservation, OpenRollback};

#[repr(transparent)]
pub struct ReservedOpen<'a, W, S, C>(Guard<'a, W, S, C>)
where
    C: OpenRollback<W, S>;

impl<'a, W, S, C> ReservedOpen<'a, W, S, C>
where
    C: OpenRollback<W, S>,
{
    pub fn new(context: &'a mut C, wire: W, send: S) -> Self {
        Self(Guard::new(context, wire, send))
    }
}

impl<W, S, C> OpenReservation<W, S> for ReservedOpen<'_, W, S, C>
where
    C: OpenRollback<W, S>,
{
    fn commit(self) -> (W, S) {
        let Self(guard) = self;
        guard.commit()
    }
}
