/// Verifies every opcode required by the native request implementation.
pub trait Inspect {
    type Code: Copy + Eq;
    type Register;

    fn inspect(&mut self) -> Result<(), Unsupported<Self::Code, Self::Register>>;
}

/// Opcode capability rejection or the exact probe registration failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Unsupported<C, E> {
    Opcode(C),
    Register(E),
}
