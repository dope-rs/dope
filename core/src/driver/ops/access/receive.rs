use std::io;

use o3::buffer::resident;

use crate::driver::{self, settings, storage::ownership};

pub(super) struct State {
    owners: ownership::Owners,
    layout: settings::Receive,
    pub(super) returned: ownership::Returned,
}

impl State {
    pub(super) fn try_new(layout: settings::Receive) -> io::Result<Self> {
        Ok(Self {
            owners: ownership::Owners::try_new(layout)?,
            layout,
            returned: ownership::Returned::try_new(layout)?,
        })
    }
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Receive<'d>(driver::Reference<'d>);

impl<'d> Receive<'d> {
    pub(in crate::driver) const fn new(driver: driver::Reference<'d>) -> Self {
        Self(driver)
    }

    pub(crate) fn retain_buffer(self, buffer: driver::Buffer) -> driver::RecvOwner<'d> {
        self.0.shared.receive.owners.acquire(buffer)
    }

    pub(crate) fn retain_recv_owner(self, owner: &driver::RecvOwner<'d>) -> driver::RecvOwner<'d> {
        self.0.shared.receive.owners.retain(owner)
    }

    pub(crate) fn retain_accounted_buffer(
        self,
        buffer: driver::Buffer,
        budget: &resident::Budget<'d>,
    ) -> Result<driver::AccountedRecvOwner<'d>, driver::Buffer> {
        self.0
            .shared
            .receive
            .owners
            .acquire_accounted(buffer, budget, self.layout().buffer_len())
    }

    pub(crate) fn retain_accounted_recv_owner(
        self,
        owner: &driver::RecvOwner<'d>,
        budget: &resident::Budget<'d>,
    ) -> Option<driver::AccountedRecvOwner<'d>> {
        self.0
            .shared
            .receive
            .owners
            .retain_accounted(owner, budget, self.layout().buffer_len())
    }

    pub(crate) fn retain_existing_accounted_recv_owner(
        self,
        owner: &driver::AccountedRecvOwner<'d>,
    ) -> driver::AccountedRecvOwner<'d> {
        self.0
            .shared
            .receive
            .owners
            .retain_existing_accounted(owner)
    }

    pub(crate) fn release_recv_owner(self, owner: &driver::RecvOwner<'d>) {
        if let Some(buffer) = self.0.shared.receive.owners.release(owner) {
            self.0.shared.receive.returned.push(buffer);
        }
    }

    pub(crate) fn release_accounted_recv_owner(self, owner: &driver::AccountedRecvOwner<'d>) {
        if let Some(buffer) = self.0.shared.receive.owners.release_accounted(owner) {
            self.0.shared.receive.returned.push(buffer);
        }
    }

    pub(crate) fn layout(self) -> settings::Receive {
        self.0.shared.receive.layout
    }
}

const _: () = assert!(
    std::mem::size_of::<Receive<'static>>() == std::mem::size_of::<driver::Reference<'static>>()
);
