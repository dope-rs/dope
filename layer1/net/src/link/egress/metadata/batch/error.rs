use crate::link::egress;

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Error<E> {
    Admission(egress::Admission),
    Build(E),
    Mismatch { remaining: egress::Credit },
}
