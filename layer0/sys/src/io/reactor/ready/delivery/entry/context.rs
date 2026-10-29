use crate::io::{
    reactor::{read, ready::delivery::entry::Owned, retry},
    socket::{Prepare, accept},
};

/// Exact host identities and values carried by a native readiness completion.
pub trait Context: Sized {
    type Reader: read::Context;
    type Writer: retry::Context<
            Slots = <Self::Reader as read::Context>::Slots,
            Key = <Self::Reader as read::Context>::Key,
        >;
    type Outbound: Copy + Into<Owned<Self>>;
    type Handle;
    type Native: Prepare<Handle = Self::Handle>;
    type Failure: From<accept::Error>;
    type Reclaimed;

    const LAST: <Self::Reader as read::Context>::Flow;
    const MORE: <Self::Reader as read::Context>::Flow;
}
