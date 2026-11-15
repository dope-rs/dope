use crate::common::runtime;
use crate::common::suite::cid::{Count, Error, INITIAL};
use crate::common::suite::peers::tls;
use dope_quic::conn::State;
use dope_quic::conn::id::Handle;
use dope_quic::conn::life::tls::clients;
use dope_quic::runtime::mux::builder::Builder;
use dope_quic::runtime::mux::builder::Plan;
use dope_quic::runtime::mux::router;
use std::mem::replace;
use std::num::NonZero;
use std::time::Duration;
use std::{net::SocketAddr, pin::Pin, time::Instant};

use dope_quic::errors::Code;
use dope_quic::proto::cid::{Rotation, Schedule};

pub struct Wire {
    server: Peer,
    client: Peer,
    active: Option<[u8; 8]>,
    now: Instant,
    authority: clients::Authority,
    rotation: Schedule,
}

struct Peer {
    router: router::Router<Count>,
    address: SocketAddr,
    handle: Handle,
}

impl Wire {
    pub fn counts(&self) -> (usize, usize, usize) {
        (
            self.server.router.active_conns(),
            self.client.router.active_conns(),
            self.server.router.handler().datagrams,
        )
    }

    pub fn new() -> Result<Self, Error> {
        let (key, config) = Count::try_keys().map_err(Error::Runtime)?;
        let auth = tls::client(key)
            .map_err(runtime::Error::Tls)
            .map_err(Error::Runtime)?;
        let plan = Plan::try_new(2, 4096, 16 << 20).map_err(Error::Limits)?;
        let mut server = Count::server(config, plan).map_err(Error::Runtime)?;
        let plan = Plan::try_new(1, 4096, 16 << 20).map_err(Error::Limits)?;
        let mut client = Builder::client(Count::default())
            .limits(plan)
            .build()
            .map_err(Error::Build)?;
        let peer = SocketAddr::from(([10, 0, 0, 2], 443));
        let local = SocketAddr::from(([10, 0, 0, 1], 50_000));
        let started = Instant::now();
        let options = Count::options().map_err(Error::Runtime)?;
        let outbound = Pin::new(&mut client)
            .observe(started, |mut turn| {
                turn.protocol()
                    .connect(peer, &auth, options, INITIAL.to_vec())
            })
            .map_err(Error::Initial)?;
        for turn in 0..16 {
            let now = started
                .checked_add(Duration::from_millis(turn * 10))
                .ok_or(Error::Runtime(runtime::Error::Deadline(started)))?;
            Count::relay_at(&mut client, &mut server, local, now).map_err(Error::Runtime)?;
            Count::relay_at(&mut server, &mut client, peer, now).map_err(Error::Runtime)?;
            if client.handler().established == 1 && server.handler().established == 1 {
                break;
            }
        }
        if client.handler().established != 1 || server.handler().established != 1 {
            return Err(Error::Setup {
                client: client.handler().established,
                server: server.handler().established,
            });
        }
        let Some(inbound) = server.handler().last_handle else {
            return Err(Error::Handle {
                established: server.handler().established,
            });
        };
        let count = NonZero::<u16>::MIN.saturating_add(1);
        let period = NonZero::<u64>::MIN.saturating_add(999_999);
        let plan = Rotation::from_nanos(period, count);
        let rotation = Pin::new(&mut server).observe(started, |turn| turn.plan_cid(plan));
        Ok(Self {
            server: Peer {
                router: server,
                address: peer,
                handle: inbound,
            },
            client: Peer {
                router: client,
                address: local,
                handle: outbound,
            },
            active: None,
            now: started
                .checked_add(Duration::from_millis(150))
                .ok_or(Error::Runtime(runtime::Error::Deadline(started)))?,
            authority: auth,
            rotation,
        })
    }

    fn now(&mut self) -> Result<Instant, Error> {
        self.now = self
            .now
            .checked_add(Duration::from_millis(10))
            .ok_or(Error::Runtime(runtime::Error::Deadline(self.now)))?;
        Ok(self.now)
    }

    pub fn probe(&mut self, sequence: u32) -> Result<(), Error> {
        let before = self.server.router.handler().datagrams;
        let now = self.now()?;
        Pin::new(&mut self.client.router)
            .observe(now, |mut turn| {
                turn.protocol()
                    .try_datagram(self.client.handle, sequence.to_be_bytes().to_vec())
            })
            .map_err(Error::ProbeSend)?;
        for _ in 0..16 {
            let now = self.now()?;
            let wires = Count::relay_at(
                &mut self.client.router,
                &mut self.server.router,
                self.client.address,
                now,
            )
            .map_err(Error::Runtime)?;
            if let Some(cid) = wires.iter().find_map(|wire| Count::short_cid(wire)) {
                self.active = Some(cid);
            }
            Count::relay_at(
                &mut self.server.router,
                &mut self.client.router,
                self.server.address,
                now,
            )
            .map_err(Error::Runtime)?;
            if self.server.router.handler().datagrams == before + 1 {
                return Ok(());
            }
        }
        Err(Error::Delivery {
            expected: before + 1,
            actual: self.server.router.handler().datagrams,
        })
    }

    pub fn rotate(&mut self) -> Result<(), Error> {
        let Some(conn) = self.server.router.conn(self.server.handle) else {
            return Err(Error::Connection {
                handle: self.server.handle,
            });
        };
        let Some(retiring) = conn
            .view()
            .path()
            .local_cids()
            .entries()
            .map(|entry| entry.0)
            .min()
        else {
            return Err(Error::Cid {
                handle: self.server.handle,
            });
        };
        let now = self.now()?;
        let progress = Pin::new(&mut self.server.router)
            .observe(now, |mut turn| turn.maintain_cid(&mut self.rotation));
        if progress.visited() != 1 || progress.started() != 1 || !progress.is_complete() {
            return Err(Error::Sweep {
                visited: progress.visited(),
                started: progress.started(),
                complete: progress.is_complete(),
            });
        }
        let Some(previous) = self.active else {
            return Err(Error::ProbeCid);
        };
        let mut next = None;
        let mut emitted = false;
        let mut retired = false;
        for _ in 0..16 {
            let now = self.now()?;
            emitted |= !Count::relay_at(
                &mut self.server.router,
                &mut self.client.router,
                self.server.address,
                now,
            )
            .map_err(Error::Runtime)?
            .is_empty();
            let wires = Count::relay_at(
                &mut self.client.router,
                &mut self.server.router,
                self.client.address,
                now,
            )
            .map_err(Error::Runtime)?;
            next = next.or_else(|| Count::changed_cid(&wires, previous));
            let Some(conn) = self.server.router.conn(self.server.handle) else {
                return Err(Error::Connection {
                    handle: self.server.handle,
                });
            };
            retired = conn
                .view()
                .path()
                .local_cids()
                .entries()
                .all(|entry| entry.0 != retiring);
            if emitted && next.is_some() && retired {
                self.active = next;
                return Ok(());
            }
        }
        Err(Error::Rotation {
            emitted,
            changed: next.is_some(),
            retired,
        })
    }

    pub fn rebind(&mut self, sequence: u32) -> Result<(), Error> {
        self.client.address.set_port(self.client.address.port() + 1);
        let before = self.server.router.handler().datagrams;
        let now = self.now()?;
        Pin::new(&mut self.client.router)
            .observe(now, |mut turn| {
                turn.protocol()
                    .try_datagram(self.client.handle, sequence.to_be_bytes().to_vec())
            })
            .map_err(Error::PathSend)?;
        let mut peer = None;
        for _ in 0..32 {
            let now = self.now()?;
            let wires = Count::relay_at(
                &mut self.client.router,
                &mut self.server.router,
                self.client.address,
                now,
            )
            .map_err(Error::Runtime)?;
            if let Some(cid) = wires.iter().find_map(|wire| Count::short_cid(wire)) {
                self.active = Some(cid);
            }
            Count::relay_at(
                &mut self.server.router,
                &mut self.client.router,
                self.server.address,
                now,
            )
            .map_err(Error::Runtime)?;
            peer = self
                .server
                .router
                .conn(self.server.handle)
                .and_then(|conn| conn.view().path().peer_address());
            if self.server.router.handler().datagrams == before + 1
                && peer == Some(self.client.address)
            {
                return Ok(());
            }
        }
        Err(Error::Path {
            expected: self.client.address,
            actual: peer,
            sent: before + 1,
            received: self.server.router.handler().datagrams,
        })
    }

    pub fn cycle(&mut self) -> Result<(), Error> {
        let cid = [1; 8];
        let mut client = Builder::client(Count::default())
            .build()
            .map_err(Error::Build)?;
        let now = self.now()?;
        let options = Count::options().map_err(Error::Runtime)?;
        let handle = Pin::new(&mut client)
            .observe(now, |mut turn| {
                turn.protocol()
                    .connect(self.server.address, &self.authority, options, cid.to_vec())
            })
            .map_err(Error::Connect)?;
        let before = self.server.router.handler().established;
        for _ in 0..16 {
            let now = self.now()?;
            Count::relay_at(
                &mut client,
                &mut self.server.router,
                self.client.address,
                now,
            )
            .map_err(Error::Runtime)?;
            Count::relay_at(
                &mut self.server.router,
                &mut client,
                self.server.address,
                now,
            )
            .map_err(Error::Runtime)?;
            if client.handler().established == 1
                && self.server.router.handler().established == before + 1
            {
                break;
            }
        }
        if client.handler().established != 1
            || self.server.router.handler().established != before + 1
        {
            return Err(Error::Replacement {
                client: client.handler().established,
                server: self.server.router.handler().established,
                expected: before + 1,
            });
        }
        if self.server.router.active_conns() != 2 || self.client.router.active_conns() != 1 {
            return Err(Error::Overlap {
                server: self.server.router.active_conns(),
                client: self.client.router.active_conns(),
            });
        }

        let Some(inbound) = self.server.router.handler().last_handle else {
            return Err(Error::Handle {
                established: self.server.router.handler().established,
            });
        };
        let old = self.client.handle;
        let mut retiring = replace(&mut self.client.router, client);
        self.client.handle = handle;
        let old_inbound = replace(&mut self.server.handle, inbound);

        let now = self.now()?;
        Pin::new(&mut retiring)
            .observe(now, |mut turn| {
                turn.protocol().with_conn(old, |mut scope| {
                    scope.lifecycle().close(Code::ZERO, Vec::new());
                })
            })
            .map_err(Error::Retire)?;
        Count::relay_at(
            &mut retiring,
            &mut self.server.router,
            self.client.address,
            now,
        )
        .map_err(Error::Runtime)?;
        Count::relay_at(
            &mut self.server.router,
            &mut retiring,
            self.server.address,
            now,
        )
        .map_err(Error::Runtime)?;
        if retiring
            .conn(old)
            .is_none_or(|conn| conn.view().lifecycle().state() != State::Closing)
            || self
                .server
                .router
                .conn(old_inbound)
                .is_none_or(|conn| conn.view().lifecycle().state() != State::Draining)
        {
            return Err(Error::Drain {
                retiring: retiring.active_conns(),
                server: self.server.router.active_conns(),
            });
        }
        let client_deadline = retiring
            .conn(old)
            .and_then(|conn| conn.view().next_timer())
            .ok_or(Error::Runtime(runtime::Error::Deadline(now)))?;
        let server_deadline = self
            .server
            .router
            .conn(old_inbound)
            .and_then(|conn| conn.view().next_timer())
            .ok_or(Error::Runtime(runtime::Error::Deadline(now)))?;
        let deadline = client_deadline.max(server_deadline);
        Pin::new(&mut retiring)
            .observe(deadline, |mut turn| turn.output().drive())
            .map_err(runtime::Error::Output)
            .map_err(Error::Runtime)?;
        Pin::new(&mut self.server.router)
            .observe(deadline, |mut turn| turn.output().drive())
            .map_err(runtime::Error::Output)
            .map_err(Error::Runtime)?;
        self.now = deadline;
        if retiring.active_conns() != 0 || self.server.router.active_conns() != 1 {
            return Err(Error::Drain {
                retiring: retiring.active_conns(),
                server: self.server.router.active_conns(),
            });
        }
        self.active = None;
        Ok(())
    }
}
