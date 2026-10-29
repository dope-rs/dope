use std::{marker, mem};

use dope_core::{
    driver::{
        retained::flight,
        route::{self, Identity, KeyTag, kind, table::entries::vacant},
    },
    io::socket::msg,
};
use o3::buffer::pool;

use crate::network::datagram::send::{self, flights};

/// Unpublished in-flight storage with initialized message backing.
pub(in crate::network::datagram) struct Stage<
    'slot,
    'd,
    ID: Identity,
    B = pool::Cursor,
    M: send::Mode = send::Plain,
> {
    slot: vacant::Staged<
        'slot,
        flights::Backing<'d, B, M>,
        <ID as route::Materialize<kind::Send>>::With,
    >,
}

/// Borrowed message view retaining the exact route class of its staged owner.
#[repr(transparent)]
pub(in crate::network::datagram) struct Draft<
    'a,
    'd,
    ID: Identity,
    B = pool::Cursor,
    M: send::Mode = send::Plain,
> {
    backing: &'a mut flights::Backing<'d, B, M>,
    tag: marker::PhantomData<fn() -> <ID as route::Materialize<kind::Send>>::With>,
}

impl<'slot, 'd, ID: Identity, B: AsRef<[u8]>, M: send::Mode> Stage<'slot, 'd, ID, B, M> {
    pub(in crate::network::datagram) fn new(
        slot: vacant::Entry<
            'slot,
            flights::Backing<'d, B, M>,
            <ID as route::Materialize<kind::Send>>::With,
        >,
        queued: super::Queued<'d, B, M>,
    ) -> Self {
        Self {
            slot: slot.stage(flights::Backing::new(queued)),
        }
    }

    pub(in crate::network::datagram) fn submit<E>(
        mut self,
        submit: impl for<'a> FnOnce(
            &mut Draft<'a, 'd, ID, B, M>,
        ) -> Result<flight::Flight<'d, kind::Send>, E>,
    ) -> Result<(), (E, super::Queued<'d, B, M>)> {
        let result = submit(&mut Draft::new(self.slot.get_mut()));
        match result {
            Ok(op) => {
                drop(op);
                self.slot.publish();
                Ok(())
            }
            Err(error) => Err((error, self.slot.reject().into_queued())),
        }
    }
}

impl<'a, 'd, ID: Identity, B: AsRef<[u8]>, M: send::Mode> Draft<'a, 'd, ID, B, M> {
    fn new(backing: &'a mut flights::Backing<'d, B, M>) -> Self {
        Self {
            backing,
            tag: marker::PhantomData,
        }
    }

    pub(in crate::network::datagram) fn fill_message(&mut self) -> msg::Message<'_> {
        self.backing.fill_message()
    }
}

const _: () = {
    assert!(mem::size_of::<Stage<'static, 'static, KeyTag<0>>>() == 2 * mem::size_of::<usize>());
    assert!(mem::size_of::<Draft<'static, 'static, KeyTag<0>>>() == mem::size_of::<usize>());
};
