use crate::table::change::Interest;

pub trait Write: Interest {
    fn cancel(self);
}
