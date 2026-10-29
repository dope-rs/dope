use dope_net::link::egress;

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Error<E> {
    Inactive,
    Admission(egress::Admission),
    Build(E),
    Mismatch { remaining: egress::Credit },
}

const _: () = {
    assert!(std::mem::size_of::<Error<()>>() == std::mem::size_of::<egress::Admission>());
    assert!(std::mem::align_of::<Error<()>>() == std::mem::align_of::<egress::Admission>());
};
