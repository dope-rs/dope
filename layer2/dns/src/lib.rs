//! Bounded DNS discovery driven by one dope application domain.

use std::{
    mem,
    net::{AddrParseError, IpAddr, SocketAddr},
    num,
};

use dope::{
    core::{driver::route, io::datagram::ReceiveError},
    manifold::{
        app::env,
        network::client::Connector,
        service::{bindings, discover, sync},
        timing,
    },
    net::{tcp, wire::Identity},
    runtime::client,
};
pub use net::stream::codec::{Error, Prefix};
use o3::{alloc, base::cell::region};
use storage::queries::index;

pub mod config;
pub mod host;
pub mod lookup;
mod net;
pub mod query;
pub mod storage;
pub mod topology;
mod wire;

use lookup::{create, refresh};
use net::{stream, udp};

type Stream<'q, 'h, 'p, 'life, 's, 'd, ID, const LANES: usize, const N: usize> = Connector<
    'q,
    'h,
    'p,
    'd,
    'd,
    ID,
    { stream::CONNECTIONS },
    bindings::Bound<
        'life,
        'd,
        storage::State<'s, 'd, LANES, N>,
        stream::Logic<'life, 's, 'd, ID, { stream::TRANSACTION_IDS }, LANES, N>,
        ID,
        { stream::CONNECTIONS },
    >,
    discover::Watched<stream::Set<'life, 's, 'd, LANES, N>>,
    sync::Replace,
    index::Server,
    (),
    env::Bundle<tcp::Tcp, Identity, timing::Balanced>,
    { config::MAX_SERVERS },
>;

/// Receives exact DNS datagram failures at the transport boundary.
///
/// Named implementations carry application policy without an indirect function
/// pointer; closures remain supported for scoped callers.
pub trait Report {
    fn receive(&mut self, cause: ReceiveError);
}

impl<F: FnMut(ReceiveError)> Report for F {
    fn receive(&mut self, cause: ReceiveError) {
        self(cause);
    }
}

#[pin_project::pin_project]
#[derive(dope::Application)]
#[dispatcher(driver = 'd)]
pub struct Resolver<
    'q,
    'h,
    'p,
    'life,
    's,
    'd,
    DatagramTag: route::Identity,
    StreamTag: route::Identity,
    const LANES: usize,
    const ADDRESSES: usize,
    R: Report,
> {
    #[pin]
    #[manifold]
    datagram: udp::Udp<'life, 's, 'd, DatagramTag, LANES, ADDRESSES, R>,
    #[pin]
    #[manifold]
    stream: Stream<'q, 'h, 'p, 'life, 's, 'd, StreamTag, LANES, ADDRESSES>,
}

pub struct Handle<'app, 's, 'd: 'app, R: ?Sized, const LANES: usize, const ADDRESSES: usize> {
    state: &'app storage::State<'s, 'd, LANES, ADDRESSES>,
    bound: client::Bound<'app, 'd, R>,
}

impl<'app, 's, 'd: 'app, R: ?Sized, const LANES: usize, const ADDRESSES: usize>
    Handle<'app, 's, 'd, R, LANES, ADDRESSES>
{
    fn new(
        state: &'app storage::State<'s, 'd, LANES, ADDRESSES>,
        bound: client::Bound<'app, 'd, R>,
    ) -> Self {
        Self { state, bound }
    }

    /// Borrows this resolver for a shorter application scope in the same driver.
    pub fn reborrow(&self) -> Handle<'_, 's, 'd, R, LANES, ADDRESSES> {
        Handle {
            state: self.state,
            bound: self.bound.borrow(),
        }
    }

    pub fn discovery<P: refresh::Policy>(
        &self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        target: Target,
    ) -> Result<
        lookup::Discovery<'app, 's, 'd, P, LANES, ADDRESSES, client::Bound<'app, 'd, R>>,
        create::Error,
    > {
        Ok(self.state.discovery(token, target)?.bind(self.bound))
    }
}

impl<
    'q,
    'h,
    'p,
    'life,
    's,
    'd: 'life,
    DatagramTag: route::Identity,
    StreamTag: route::Identity,
    const LANES: usize,
    const ADDRESSES: usize,
    R: Report,
> client::Provider<'d>
    for Resolver<'q, 'h, 'p, 'life, 's, 'd, DatagramTag, StreamTag, LANES, ADDRESSES, R>
{
    type Client<'app>
        = Handle<'app, 's, 'd, Self, LANES, ADDRESSES>
    where
        Self: 'app,
        'd: 'app;

    fn provide<'app>(context: client::Context<'_, 'app, 'd, Self>) -> Self::Client<'app>
    where
        Self: 'app,
        'd: 'app,
    {
        let (provider, bound) = context.into_parts();
        Handle::new(provider.get_ref().stream.session().root(), bound)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Host {
    Name(Box<host::Name>),
    Address(SocketAddr),
}

#[derive(Debug, PartialEq, Eq)]
pub struct Target {
    host: Host,
    port: num::NonZero<u16>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TargetRef<'a> {
    host: HostRef<'a>,
    port: num::NonZero<u16>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostRef<'a> {
    Name(&'a host::Name),
    Address(SocketAddr),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TargetError {
    MissingPort,
    BracketedPort,
    InvalidPort(num::ParseIntError),
    EmptyHost,
    ZeroPort,
    Host {
        ip: AddrParseError,
        name: host::Error,
    },
    Exhausted(alloc::Exhausted),
}

impl Target {
    pub fn parse(input: &str) -> Result<Self, TargetError> {
        if let Ok(address) = input.parse::<SocketAddr>() {
            return Self::try_from(address);
        }
        let (host, port) = if let Some(rest) = input.strip_prefix('[') {
            let Some((host, port)) = rest.split_once("]:") else {
                return Err(TargetError::BracketedPort);
            };
            (host, port)
        } else {
            let (host, port) = input.rsplit_once(':').ok_or(TargetError::MissingPort)?;
            if host.contains(':') {
                return Err(TargetError::BracketedPort);
            }
            (host, port)
        };
        let port = port.parse::<u16>().map_err(TargetError::InvalidPort)?;
        Self::try_new(host, port)
    }

    /// Constructs a service target without formatting and reparsing an
    /// intermediate authority string.
    pub fn try_new(host: &str, port: u16) -> Result<Self, TargetError> {
        if host.is_empty() {
            return Err(TargetError::EmptyHost);
        }
        let port = num::NonZero::<u16>::new(port).ok_or(TargetError::ZeroPort)?;
        let host = match host.parse::<IpAddr>() {
            Ok(ip) => Host::Address(SocketAddr::new(ip, port.get())),
            Err(ip) => {
                let name =
                    host::Name::parse(host).map_err(|name| TargetError::Host { ip, name })?;
                Host::Name(alloc::BoxExt::try_new(name).map_err(TargetError::Exhausted)?)
            }
        };
        Ok(Self { host, port })
    }

    pub fn try_clone(&self) -> Result<Self, alloc::Exhausted> {
        let host = match &self.host {
            Host::Name(name) => Host::Name(alloc::BoxExt::try_new((**name).clone())?),
            Host::Address(address) => Host::Address(*address),
        };
        Ok(Self {
            host,
            port: self.port,
        })
    }

    pub const fn is_name(&self) -> bool {
        matches!(self.host, Host::Name(_))
    }

    pub fn as_ref(&self) -> TargetRef<'_> {
        let host = match &self.host {
            Host::Name(name) => HostRef::Name(name),
            Host::Address(address) => HostRef::Address(*address),
        };
        TargetRef {
            host,
            port: self.port,
        }
    }

    pub(crate) fn into_parts(self) -> (Host, num::NonZero<u16>) {
        (self.host, self.port)
    }
}

impl<'a> TargetRef<'a> {
    pub const fn host(self) -> HostRef<'a> {
        self.host
    }

    pub const fn port(self) -> num::NonZero<u16> {
        self.port
    }

    pub(crate) const fn named(name: &'a host::Name, port: num::NonZero<u16>) -> Self {
        Self {
            host: HostRef::Name(name),
            port,
        }
    }

    pub(crate) const fn literal(address: SocketAddr, port: num::NonZero<u16>) -> Self {
        Self {
            host: HostRef::Address(address),
            port,
        }
    }
}

impl TryFrom<SocketAddr> for Target {
    type Error = TargetError;

    fn try_from(addr: SocketAddr) -> Result<Self, Self::Error> {
        Ok(Self {
            host: Host::Address(addr),
            port: num::NonZero::<u16>::new(addr.port()).ok_or(TargetError::ZeroPort)?,
        })
    }
}

const _: () = {
    assert!(mem::size_of::<num::NonZero<u16>>() == mem::size_of::<u16>());
    assert!(mem::align_of::<num::NonZero<u16>>() == mem::align_of::<u16>());
    assert!(mem::size_of::<Option<num::NonZero<u16>>>() == mem::size_of::<u16>());
    assert!(mem::size_of::<Host>() <= mem::size_of::<SocketAddr>() + mem::size_of::<usize>());
    assert!(mem::size_of::<Target>() <= mem::size_of::<SocketAddr>() + 2 * mem::size_of::<usize>());
    assert!(
        mem::size_of::<TargetRef<'static>>()
            <= mem::size_of::<SocketAddr>() + 2 * mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<Handle<'static, 'static, 'static, (), 1, 1>>()
            == mem::size_of::<&storage::State<'static, 'static, 1, 1>>()
    );
    assert!(
        mem::align_of::<Handle<'static, 'static, 'static, (), 1, 1>>()
            == mem::align_of::<&storage::State<'static, 'static, 1, 1>>()
    );
    assert!(
        mem::size_of::<Option<Handle<'static, 'static, 'static, (), 1, 1>>>()
            == mem::size_of::<&storage::State<'static, 'static, 1, 1>>()
    );
    assert!(!mem::needs_drop::<
        Handle<'static, 'static, 'static, (), 1, 1>,
    >());
};
