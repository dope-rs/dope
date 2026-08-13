use crate::{config, storage::state::indices};

#[must_use]
pub(in crate::storage) struct Lease(());

impl Lease {
    pub(in crate::storage) const fn acquired() -> Self {
        Self(())
    }
}

#[repr(u8)]
pub(super) enum Mode {
    Datagram,
    Stream(Lease),
}

pub(in crate::storage) trait Registry {
    fn acquire(&mut self, server: indices::ServerIndex) -> Lease;

    fn transfer(
        &mut self,
        lease: Lease,
        from: indices::ServerIndex,
        to: indices::ServerIndex,
    ) -> Lease;

    fn release(&mut self, lease: Lease, server: indices::ServerIndex);
}

impl Registry for [usize; config::MAX_SERVERS] {
    fn acquire(&mut self, server: indices::ServerIndex) -> Lease {
        self[server.get()] += 1;
        Lease::acquired()
    }

    fn transfer(
        &mut self,
        lease: Lease,
        from: indices::ServerIndex,
        to: indices::ServerIndex,
    ) -> Lease {
        if from != to {
            self[from.get()] -= 1;
            self[to.get()] += 1;
        }
        lease
    }

    fn release(&mut self, _lease: Lease, server: indices::ServerIndex) {
        self[server.get()] -= 1;
    }
}

const _: () = assert!(std::mem::size_of::<Lease>() == 0);
const _: () = assert!(!std::mem::needs_drop::<Lease>());
