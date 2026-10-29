use crate::io::fd::{domain, domain::resident, handles};

pub(super) fn get<'a>(
    domain: &'a domain::Domain,
    slot: handles::PhysicalSlot,
) -> resident::Lifecycle<'a> {
    domain.resident.lifecycle(slot)
}

pub(super) fn get_index(domain: &domain::Domain, index: domain::Index) -> resident::Lifecycle<'_> {
    domain.resident.mapped(index)
}
