use crate::io::{
    reactor::ready::{Driver, delivery::entry, store::Exec},
    request,
};

pub trait Context: Driver {
    type Outbound: Copy;
    type Handle;
    type Created: entry::Create<
            Self::Outbound,
            Self::Handle,
            Key = <Self::Context as Exec>::Key,
            Output = <Self::Context as Exec>::Event,
        >;
    type File: request::Complete<Output = <Self::Context as Exec>::Event>;

    fn file(key: <Self::Context as Exec>::Key) -> Self::File;
}
