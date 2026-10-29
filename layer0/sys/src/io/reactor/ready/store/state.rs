use crate::{
    Errno,
    io::{
        reactor::ready::store::{Context, Exec, Parts},
        socket::option,
    },
    table::{change, registry},
};

type View<'a, C> = Parts<
    &'a <C as Context>::Writes,
    &'a <C as Context>::Files,
    &'a <C as Context>::Poll,
    &'a <C as Context>::Reads,
    &'a <C as Context>::Pool,
>;
type Edit<'a, C> = Parts<
    &'a mut <C as Context>::Writes,
    &'a mut <C as Context>::Files,
    &'a mut <C as Context>::Poll,
    &'a mut <C as Context>::Reads,
    &'a <C as Context>::Pool,
>;

pub trait State<C: Context>: Sized {
    fn new(
        retries: C::Writes,
        files: C::Files,
        poll: C::Poll,
        reads: C::Reads,
        recv: C::Pool,
    ) -> Self;
    fn view(&self) -> View<'_, C>;
    fn edit(&mut self) -> Edit<'_, C>;
    fn set<O: option::Cursor>(
        &self,
        slot: <C::Slots as change::raw::Slots>::Physical,
        options: Option<O>,
    ) -> Result<(), Errno>;
    fn read<'a>(&'a mut self) -> C::Arm<'a>
    where
        C: Exec + 'a;
    fn write<'a>(&'a mut self) -> C::Retry<'a>
    where
        C: Exec + 'a;
    fn poll<'a>(&'a mut self, query: registry::Query<C::Read>) -> Option<C::ReadPoll<'a>>
    where
        C: Exec + 'a;
}
