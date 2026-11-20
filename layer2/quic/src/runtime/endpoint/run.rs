#![deny(unsafe_code)]

use std::{marker, pin};

use app::step;
use dope::{
    core::driver::{
        self, route,
        schedule::{self, app, clock},
    },
    manifold::network::datagram::{self, hook},
};
use o3::{
    base::cell::region,
    buffer::{self, resident},
    mem::quota,
};

use crate::{
    conn::id,
    runtime::{
        endpoint::{Payload, Socket, Transport, reactor},
        mux::{
            self, auth, handler,
            handler::hook::{
                set,
                wire::{cert, packet},
            },
        },
        receive::Storage,
    },
};

use mux::app::{BATCH_QUANTUM, Driver, Receipt, Silent, drive, has_work};
use mux::output::quantum::Quantum;
use reactor::{Engine, Work};

/// Application scheduler and its endpoint-owned fair-work cursor.
pub struct Run<A, R = Silent> {
    pub(in crate::runtime::endpoint) app: A,
    pub(in crate::runtime::endpoint) work: reactor::WorkGraph,
    receipt: marker::PhantomData<fn() -> R>,
}

impl<A, R> Run<A, R> {
    pub(in crate::runtime::endpoint) fn new(app: A) -> Self {
        Self {
            app,
            work: reactor::WorkGraph::new(),
            receipt: marker::PhantomData,
        }
    }
}

impl<'d, const ID: u8, H, B, M, Q, A, R>
    hook::Schedule<'d, route::KeyTag<ID>, reactor::Engine<'d, H, ID, B, Run<A, R>, M, Q>>
    for Run<A, R>
where
    H: handler::Handler<ID, B, resident::Shared<'d>, id::Driver<'d>>,
    B: Storage<'d, resident::Shared<'d>>,
    M: auth::Mode<ID, Send = resident::Shared<'d>, Identity = id::Driver<'d>>,
    Q: buffer::Residency + buffer::raw::Source,
    A: Driver<'d, H, ID, B, resident::Shared<'d>>,
    R: Receipt<'d, H, ID, B, resident::Shared<'d>, A>,
    H: cert::Sink<ID, id::Driver<'d>>,
    <H::Events as set::Dispatch<H, ID, B, resident::Shared<'d>, id::Driver<'d>>>::Packet:
        packet::Mode<H, ID, B, resident::Shared<'d>, id::Driver<'d>, Output = ()>,
{
    type Plan<'turn> = <<Socket<'d, ID, H, B> as set::Socket<
        H,
        ID,
        B,
        resident::Shared<'d>,
        id::Driver<'d>,
    >>::Application<'d, ID, reactor::Engine<'d, H, ID, B, Run<A, R>, M, Q>> as hook::Schedule<
        'd,
        route::KeyTag<ID>,
        reactor::Engine<'d, H, ID, B, Run<A, R>, M, Q>,
    >>::Plan<'turn>;

    fn select<'turn>(clock: clock::Clock<'turn, 'd>) -> Option<Self::Plan<'turn>> {
        <<Socket<'d, ID, H, B> as set::Socket<H, ID, B, resident::Shared<'d>, id::Driver<'d>>>::Application<'d, ID, reactor::Engine<'d, H, ID, B, Run<A, R>, M, Q>> as hook::Schedule<'d, route::KeyTag<ID>, reactor::Engine<'d, H, ID, B, Run<A, R>, M, Q>>>::select(clock)
    }

    fn progress(
        endpoint: pin::Pin<&Transport<'d, ID, H, B, Run<A, R>, M, Q>>,
        plan: Self::Plan<'_>,
        token: &region::Token<region::Identity<'d, 1>>,
    ) -> schedule::Progress<'d> {
        <<Socket<'d, ID, H, B> as set::Socket<H, ID, B, resident::Shared<'d>, id::Driver<'d>>>::Application<'d, ID, reactor::Engine<'d, H, ID, B, Run<A, R>, M, Q>> as hook::Schedule<'d, route::KeyTag<ID>, reactor::Engine<'d, H, ID, B, Run<A, R>, M, Q>>>::progress(endpoint, plan, token)
    }
}

impl<'d, const ID: u8, H, B, A, R, M, Q: buffer::Residency + buffer::raw::Source>
    datagram::Application<'d, route::KeyTag<ID>> for Engine<'d, H, ID, B, Run<A, R>, M, Q>
where
    H: handler::Handler<ID, B, resident::Shared<'d>, id::Driver<'d>>,
    B: Storage<'d, resident::Shared<'d>>,
    A: Driver<'d, H, ID, B, resident::Shared<'d>>,
    R: Receipt<'d, H, ID, B, resident::Shared<'d>, A>,
    M: auth::Mode<ID, Send = resident::Shared<'d>, Identity = id::Driver<'d>>,
    H: cert::Sink<ID, id::Driver<'d>>,
    <H::Events as set::Dispatch<H, ID, B, resident::Shared<'d>, id::Driver<'d>>>::Packet:
        packet::Mode<H, ID, B, resident::Shared<'d>, id::Driver<'d>, Output = ()>,
{
    fn pre_park<'turn>(
        self: pin::Pin<&mut Self>,
        mut socket: pin::Pin<&mut datagram::Socket<'d, route::KeyTag<ID>, Payload<Q>>>,
        now: clock::Moment<'d>,
        work: app::Work<'turn, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let this = self.project();
        let mut mux = this.mux;
        if !this.io.begin(mux.as_mut(), now, work) {
            return;
        }
        let mut quantum = Quantum::new();
        let app = &mut this.app.app;
        while let quota::Admission::Item(false) = this.app.work.admit(|source| match source {
            Work::Application => {
                if !has_work(mux.as_ref(), app) {
                    return quota::Admission::Empty;
                }
                let parent = socket.as_ref().wake_target();
                for driven in 0..usize::from(BATCH_QUANTUM.get()) {
                    match drive(mux.as_mut(), app, parent, work, driver, now.into()) {
                        quota::Admission::Empty if driven == 0 => return quota::Admission::Empty,
                        quota::Admission::Exhausted if driven == 0 => {
                            return quota::Admission::Exhausted;
                        }
                        quota::Admission::Exhausted => return quota::Admission::Item(true),
                        quota::Admission::Empty | quota::Admission::Item(step::Flow::Yield) => {
                            break;
                        }
                        quota::Admission::Item(step::Flow::Batch) => {}
                    }
                }
                quota::Admission::Item(false)
            }
            Work::Output => this.io.output::<R, _, _>(
                mux.as_mut(),
                socket.as_mut(),
                this.rotation,
                now,
                work,
                &mut quantum,
                app,
            ),
            Work::Transport => Self::transport(mux.as_mut(), now, work),
        }) {}
    }

    fn progress(
        self: pin::Pin<&Self>,
        clock: clock::Clock<'_, 'd>,
        _: &region::Token<region::Identity<'d, 1>>,
    ) -> schedule::Progress<'d> {
        self.progress(clock, |state| {
            let this = self.project_ref();
            if has_work(this.mux, &this.app.app) {
                state.reduce(schedule::Progress::Runnable)
            } else {
                state
            }
        })
    }
}
