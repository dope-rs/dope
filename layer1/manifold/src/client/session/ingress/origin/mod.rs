use dope_core::driver::settings::Domain;

mod seal;

pub(crate) use seal::Seal;

pub trait Policy: Seal {
    const DOMAINS: Domain;
}

pub struct Local;

pub struct External;

impl Seal for Local {}

impl Seal for External {}

impl Policy for Local {
    const DOMAINS: Domain = Domain::ONE;
}

impl Policy for External {
    const DOMAINS: Domain = Domain::ZERO;
}
