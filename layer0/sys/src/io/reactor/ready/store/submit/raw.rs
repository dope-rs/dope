use crate::io::reactor::{
    complete::Vacancy,
    ready::store::{Exec, submit::Context},
};

pub trait Retain<C: Context> {
    /// # Safety
    /// Every submitted pointee remains live with its admitted bounds until its flight settles.
    unsafe fn retain<'a>(
        self,
        state: &'a mut C::State,
        key: <C::Context as Exec>::Key,
        vacant: impl Vacancy<Event = <C::Context as Exec>::Event>,
    ) where
        C::Context: 'a;
}
