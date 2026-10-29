use dope_core::{
    dope_sys::{io::socket::msg::source, thread::timer},
    driver::{
        route::lease,
        schedule::{clock::timer::Exhausted, ready, sequence},
        storage,
    },
    io::fd::outbound,
};
use dope_net::link::egress::create;
use o3::{
    alloc,
    buffer::{
        pool::GrowingError,
        resident::{AllocError, staging},
    },
    depot::{
        heap,
        queue::{resident, tagged},
    },
};

use crate::{
    client::attempt,
    file::appender,
    service::{self, health},
};

/// Application construction failure with exact domain categories.
///
/// An unrelated operating-system failure cannot be erased into this type:
///
/// ```compile_fail
/// use dope_manifold::app::creation::Error;
///
/// struct Unrelated;
/// let error = Unrelated;
/// let converted: Error = error.into();
/// ```

#[derive(Debug)]
pub enum Error {
    Config(crate::ConfigError),
    Allocation(alloc::Error),
    Queue(tagged::Error),
    Heap(heap::Error),
    Exhausted(alloc::Exhausted),
    Resident(storage::Exhausted),
    Ingress(staging::Error),
    IngressStorage(AllocError),
    Ready(ready::Capacity),
    Wake(resident::Error),
    RootExhausted(sequence::Exhausted),
    Timer(Exhausted),
    Credit(alloc::Error),
    Attempt(attempt::ResizeError),
    Route(lease::Error),
    Descriptor(outbound::Error),
    Network(dope_net::ConfigError),
    Binding(dope_net::BindingError),
    DatagramSource(source::Error),
    BufferLayout(dope_net::BufferLayout),
    BufferPool(GrowingError),
    Egress(create::Error),
    Service(service::Reconcile),
    Pool(service::Error),
    Retry(health::Error),
    Appender(appender::OpenError),
    Watchdog(timer::Error),
}

impl From<dope_net::Resource> for Error {
    fn from(error: dope_net::Resource) -> Self {
        match error {
            dope_net::Resource::Config(error) => Self::Network(error),
            dope_net::Resource::Descriptor(error) => Self::Descriptor(error),
            dope_net::Resource::Binding(error) => Self::Binding(error),
            dope_net::Resource::BufferLayout(error) => Self::BufferLayout(error),
            dope_net::Resource::Pool(error) => Self::BufferPool(error),
            dope_net::Resource::Allocation(error) => Self::Allocation(error),
            dope_net::Resource::Queue(error) => Self::Queue(tagged::Error::Allocation(error)),
            dope_net::Resource::Heap(error) => Self::Heap(error),
            dope_net::Resource::Exhausted(error) => Self::Exhausted(error),
            dope_net::Resource::Credit(error) => Self::Credit(error),
            dope_net::Resource::Egress(error) => Self::Egress(error),
        }
    }
}
