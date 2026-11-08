/// Installs a descriptor in its reserved native table slot.
pub trait Install<O, H> {
    type Error;

    fn install(&mut self, slot: O, handle: H) -> Result<(), Self::Error>;
}
