use crate::driver::schedule::ready;

#[repr(transparent)]
pub(in crate::driver::schedule::ready) struct Permit<'d>(ready::ReservedKey<'d>);

impl<'d> Permit<'d> {
    pub(in crate::driver::schedule::ready) fn new(key: ready::ReservedKey<'d>) -> Self {
        Self(key)
    }

    pub(in crate::driver::schedule::ready) fn into_key(self) -> ready::ReservedKey<'d> {
        self.0
    }

    pub(in crate::driver::schedule::ready) fn install(
        self,
        slots: &'d ready::Slots,
        root: &ready::TopologyKey<'d>,
        task: ready::Task<'d>,
    ) -> ready::TaskKey<'d> {
        let staged = slots.mutation().task(self.into_key(), task);
        ready::Topology::get(slots, root).attach(staged)
    }
}
