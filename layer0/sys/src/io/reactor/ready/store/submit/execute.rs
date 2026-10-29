use crate::io::reactor::{
    complete::Vacancy,
    ready::store::{Exec, submit::Context},
};

pub trait Execute<C: Context> {
    fn execute<'a>(
        self,
        state: &'a mut C::State,
        key: <C::Context as Exec>::Key,
        vacant: impl Vacancy<Event = <C::Context as Exec>::Event>,
    ) where
        C::Context: 'a;
}
