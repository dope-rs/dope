//! Typed startup plans for DNS transport resources and installation.
use std::mem;

use dope::{
    core::{
        driver::{
            route::{Identity, KeyTag},
            settings,
            settings::{claim, errors},
            storage::{Build, Path, path},
        },
        io::datagram::ReceiveError,
    },
    manifold,
    manifold::{
        app::{creation, env},
        client::session,
        network::{
            client::topology::plan::{Layout, Spec},
            datagram::{plan, send},
        },
        service::{bindings, health, sync},
        timing,
    },
    net::{link::egress, tcp, wire},
    runtime::{executor, random, topology::lending},
};
use o3::{alloc, buffer::pool};
use session::ingress;
use wire::domain;

use crate::{
    Report, config,
    net::{entropy, stream, udp},
    storage::{self, queries::index},
    wire::frames,
};

#[repr(transparent)]
struct Egress(egress::Config);

/// Failure to prepare a resolver topology plan.
#[derive(Debug)]
pub enum Prepare {
    Config(crate::config::Error),
    Datagram(manifold::ConfigError),
    Stream(errors::Claim),
    Egress {
        required_entries: egress::Entries,
        available_entries: egress::Entries,
        required_resident: egress::Resident,
        available_resident: egress::Resident,
    },
    Allocation(alloc::Exhausted),
    BufferLayout(pool::LayoutError),
}

impl Egress {
    const ENTRIES: egress::Entries = egress::Entries::new(1);
    const RESIDENT: egress::Resident = egress::Resident::new(frames::MAX_STREAM_FRAME_BYTES as u32);

    fn try_new(config: egress::Config) -> Result<Self, Prepare> {
        let entries = config.entries();
        let resident = config.bytes();
        if entries < Self::ENTRIES || resident < Self::RESIDENT {
            return Err(Prepare::Egress {
                required_entries: Self::ENTRIES,
                available_entries: entries,
                required_resident: Self::RESIDENT,
                available_resident: resident,
            });
        }
        Ok(Self(config))
    }

    const fn into_inner(self) -> egress::Config {
        self.0
    }
}

/// Complete transport plan for one resolver over an exact storage path.
pub struct Plan<
    DatagramTag: Identity,
    StreamTag: Identity,
    const OUTBOUND: usize,
    const LANES: usize,
    const ADDRESSES: usize,
    R,
    P = path::Here,
> {
    path: P,
    report: R,
    packets: udp::Pool,
    datagram: plan::Plan<DatagramTag, send::Sourced>,
    stream: Layout<StreamTag, 1>,
    egress: Egress,
}

impl<
    DatagramTag: Identity,
    StreamTag: Identity,
    const OUTBOUND: usize,
    const LANES: usize,
    const ADDRESSES: usize,
    R: Report,
> Plan<DatagramTag, StreamTag, OUTBOUND, LANES, ADDRESSES, R>
{
    pub fn new(egress: egress::Config, report: R) -> Result<Self, Prepare> {
        Self::at(path::Here, egress, report)
    }
}

impl<
    DatagramTag: Identity,
    StreamTag: Identity,
    const OUTBOUND: usize,
    const LANES: usize,
    const ADDRESSES: usize,
    R,
    P,
> Plan<DatagramTag, StreamTag, OUTBOUND, LANES, ADDRESSES, R, P>
{
    pub fn at(path: P, egress: egress::Config, report: R) -> Result<Self, Prepare>
    where
        R: Report,
    {
        let bytes = OUTBOUND
            .checked_mul(frames::MAX_QUERY_BYTES)
            .ok_or(Prepare::Config(config::Error::OutboundBytes {
                outbound: OUTBOUND,
                per_query: frames::MAX_QUERY_BYTES,
            }))?;
        let config = plan::Config::new(OUTBOUND, bytes, OUTBOUND).map_err(Prepare::Datagram)?;
        let layout = pool::Width::new(OUTBOUND).map_err(Prepare::BufferLayout)?;
        let datagram = plan::Plan::new(config);
        let stream = Layout::new(1).map_err(Prepare::Stream)?;
        let egress = Egress::try_new(egress)?;
        let packets = udp::Pool::try_from(layout).map_err(Prepare::Allocation)?;
        Ok(Self {
            path,
            report,
            packets,
            datagram,
            stream,
            egress,
        })
    }
}

impl<
    DatagramTag: Identity,
    StreamTag: Identity,
    const OUTBOUND: usize,
    const LANES: usize,
    const ADDRESSES: usize,
    R,
    P,
> settings::Resource for Plan<DatagramTag, StreamTag, OUTBOUND, LANES, ADDRESSES, R, P>
{
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        let needs =
            needs.include(self.datagram.claim().receive(
                settings::Datagram::fixed::<2, { frames::EDNS_PAYLOAD as u32 }>().into(),
            ))?;
        settings::Resource::include_in(&self.stream, needs)
    }
}

impl<
    'stage,
    'scope,
    'life: 'scope,
    'e: 'life,
    'd: 'e,
    Root,
    R,
    P,
    Q,
    N,
    O,
    DatagramTag: Identity,
    StreamTag: Identity,
    const OUTBOUND: usize,
    const LANES: usize,
    const ADDRESSES: usize,
> lending::Installer<lending::Stage<'stage, 'scope, 'life, 'e, 'd, Root, Q>, N>
    for Plan<DatagramTag, StreamTag, OUTBOUND, LANES, ADDRESSES, R, P>
where
    StreamTag: 'd,
    Root: Build + 'd,
    R: Report + 'd,
    P: Path<Root, storage::Plan<LANES, ADDRESSES>> + 'd,
    Q: executor::Startup,
    N: for<'q, 'h, 'p> lending::Next<
            lending::Stage<'stage, 'scope, 'life, 'e, 'd, Root, Q>,
            crate::Resolver<'q, 'h, 'p, 'life, 'e, 'd, DatagramTag, StreamTag, LANES, ADDRESSES, R>,
            Output = O,
        >,
{
    type Output = O;
    type Error = creation::Error;

    fn install(
        self,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, Root, Q>,
        next: N,
    ) -> Result<O, Self::Error> {
        let storage = self.path.get(stage.storage());
        let config = &storage.queries.config;
        let stream = self.stream.into_plan(
            Spec::new(config.refresh.backoff.base, config.refresh.backoff.max)
                .map_err(creation::Error::Retry)?
                .with_egress(self.egress.into_inner()),
        );
        let udp_rng = entropy::Datagram::new(
            random::Root::try_acquire(&stage.driver()).map_err(creation::Error::RootExhausted)?,
        );
        let datagram = udp::Udp::bind(
            self.datagram,
            self.packets,
            storage,
            config.bind,
            udp_rng,
            self.report,
            &mut stage.driver(),
        )?;
        let tcp_rng = entropy::Stream::new(
            random::Root::try_acquire(&stage.driver()).map_err(creation::Error::RootExhausted)?,
        );
        let session = bindings::Bound::bind(storage, stream::Logic::new(tcp_rng));
        let setup = session::Setup::new(session, ingress::Plan::none());
        let peers = stream::Set::new(storage);
        let set = peers.snapshot();
        let hash = stage.hash_state(health::Domain::for_id(StreamTag::ID.raw()));
        let stream = stream.bind(domain::Local::new(wire::Identity), hash);
        stream.watch::<
            _, _, _, sync::Replace, index::Server,
            env::Bundle<tcp::Tcp, wire::Identity, timing::Balanced>,
            { config::MAX_SERVERS }, _, _,
        >(
            setup, set, peers,
            stage, lending::Stage::driver,
            |stream, stage| next.run(crate::Resolver { datagram, stream }, stage),
        )
    }
}

const _: () = {
    assert!(mem::size_of::<Egress>() == mem::size_of::<egress::Config>());
    assert!(mem::align_of::<Egress>() == mem::align_of::<egress::Config>());
    assert!(
        mem::size_of::<Plan<KeyTag<0>, KeyTag<1>, 1, 1, 1, fn(ReceiveError)>>()
            == mem::size_of::<(
                udp::Pool,
                plan::Plan<KeyTag<0>, send::Sourced>,
                Layout<KeyTag<1>, 1>,
                egress::Config,
                fn(ReceiveError)
            )>()
    );
};
