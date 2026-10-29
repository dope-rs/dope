use crate::table::change;

pub trait Interest {
    type Id: change::Id;
    type Data: Copy;

    fn id(&self) -> Self::Id;
    fn arm(self, flags: u16, data: Self::Data);
    fn delete(self);
    fn discard(self);
}
