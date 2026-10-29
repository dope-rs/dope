use std::{net, ops::ControlFlow, pin};

use dope_core::{
    driver::{
        result,
        retained::{self, flight},
        route::{self, Identity, indices::SlotIndex, kind::Recv, lease},
        schedule::{self, app},
    },
    io::{
        self,
        datagram::ReceiveError,
        event::{self, net::recv::Completion},
    },
};
use dope_runtime::executor;
use o3::{buffer::pool, depot::queue::indexed::fifo};

use crate::{
    app::dispatch::{
        self,
        event::{Complete, Observe},
    },
    network::datagram::{
        group::{Access, Handler, Member, View, mark},
        send,
        socket::sender::Sender,
    },
};

#[pin_project::pin_project(!Unpin)]
pub struct Endpoint<'d, ID: Identity, H> {
    pub(super) members: Box<[Member<'d, ID>]>,
    pub(super) sender: Sender<'d, ID, pool::Cursor, send::Routed>,
    pub(super) flights: flight::Slots<'d, <ID as route::Materialize<Recv>>::With>,
    pub(super) owner: lease::Owner<'d, ID>,
    pub(super) ready: fifo::List<SlotIndex>,
    pub(super) deferred: bool,
    pub(super) send_first: bool,
    #[pin]
    pub(super) handler: H,
}

enum Failure {
    Receive(SlotIndex, result::SubmitError),
    Send(result::SubmitError),
}

impl<'d, ID: Identity, H: Handler<'d, ID>> Complete<'d> for Endpoint<'d, ID, H> {
    fn dispatch<'app>(
        call: executor::raw::Call<'app, Self, (), io::Event<'d>>,
    ) -> ControlFlow<io::Event<'d>> {
        let (mut endpoint, (), event) = call.into_parts();
        let this = endpoint.as_mut().project();
        let done = match event.into_kind() {
            event::Kind::Recv(done) => {
                if route::Id::try_from(done.token().route()) != Ok(ID::ID) {
                    return ControlFlow::Break(event::Kind::Recv(done).into());
                }
                let source = done.token().slot();
                let Some(member) = this.members.get_mut(source.index().get() as usize) else {
                    return ControlFlow::Break(event::Kind::Recv(done).into());
                };
                let received = pin::Pin::new(&mut member.receive).complete(done);
                if !member.receive.is_live() {
                    mark(this.ready, source);
                }
                let received = received.map_break(Completion::retire)?;
                received.dispatch(|received| {
                    this.handler.packet(
                        received,
                        source,
                        Access {
                            sender: this.sender,
                            ready: this.ready,
                            members: this.members,
                        },
                    );
                });
                return ControlFlow::Continue(());
            }
            event::Kind::Send(done) => done,
            event => return ControlFlow::Break(event.into()),
        };
        match this.sender.retire().complete(done) {
            Ok(outcome) => {
                if let Some(outcome) = outcome {
                    this.handler.sent(outcome);
                }
                ControlFlow::Continue(())
            }
            Err(done) => ControlFlow::Break(event::Kind::Send(done).into()),
        }
    }
}

// SAFETY: the installed group owns every immutable member, descriptor and queued
// payload through terminal completion or the driver's quiescent finish.
unsafe impl<'d, ID: Identity, H: Handler<'d, ID>> dispatch::raw::Manifold<'d>
    for Endpoint<'d, ID, H>
{
    type Route = ID;
    type Dispatch = Observe;
    type Install = dispatch::raw::Omit;
    type Finish = Self;
    type Activate = dispatch::raw::Plain;
    type Probe = Self;
    type PrePark = dispatch::raw::Retained;
    type Shutdown = dispatch::raw::Plain;

    const COORDS: usize = 0;

    /// A zero-budget pass must not inherit an earlier admission failure.
    unsafe fn pre_park<'a, 'turn, 'root>(
        mut self: pin::Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'root, 'd, Self::PrePark>,
    ) -> pin::Pin<&'a mut Self>
    where
        Self: 'root,
    {
        let mut this = self.as_mut().project();
        if !this.sender.is_accepting() {
            this.sender
                .retire()
                .drain(turn.reborrow().maint(), |error| {
                    this.handler.as_mut().sent(send::Outcome::Submit(error));
                });
            for member in this.members.iter_mut() {
                pin::Pin::new(&mut member.receive).retry_stop(driver);
            }
            return self;
        }
        *this.deferred = false;
        let first = *this.send_first;
        if turn.app().remaining() != 0 {
            *this.send_first = !first;
        }
        let result = if first {
            self.as_mut()
                .flush(turn.app(), driver)
                .and_then(|()| self.as_mut().arm(turn.app(), driver))
        } else {
            self.as_mut()
                .arm(turn.app(), driver)
                .and_then(|()| self.as_mut().flush(turn.app(), driver))
        };
        if let Err(failure) = result {
            self.as_mut().fail(failure, driver);
        }
        self
    }

    fn shutdown<'a, 'turn, 'root>(
        mut self: pin::Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'root, 'd, Self::Shutdown>,
    ) -> pin::Pin<&'a mut Self>
    where
        Self: 'root,
    {
        let mut this = self.as_mut().project();
        this.sender.stop();
        for member in this.members.iter_mut() {
            pin::Pin::new(&mut member.receive).stop(driver);
        }
        this.sender.retire().drain(turn.maint(), |error| {
            this.handler.as_mut().sent(send::Outcome::Submit(error));
        });
        self
    }
}

impl<'d, ID: Identity, H> Endpoint<'d, ID, H> {
    pub fn handler(&self) -> &H {
        &self.handler
    }

    pub fn local_addr(&self, slot: SlotIndex) -> Option<net::SocketAddr> {
        self.members
            .get(slot.index().get() as usize)
            .map(|member| member.addr)
    }

    fn arm<'owner>(
        self: pin::Pin<&mut Self>,
        work: app::Work<'_, 'd>,
        driver: &mut retained::Context<'_, 'owner, 'd>,
    ) -> Result<(), Failure>
    where
        'd: 'owner,
    {
        let this = self.project();
        while work.remaining() != 0 {
            let Some(slot) = this.ready.pop_front() else {
                break;
            };
            // SAFETY: only the same fixed-domain queue admits these indices;
            // members retain its exact extent throughout installed execution.
            let member = unsafe { this.members.get_unchecked_mut(slot.index().get() as usize) };
            pin::Pin::new(&mut member.receive)
                .arm(&member.fd, this.flights, work, driver)
                .map_err(|error| Failure::Receive(slot, error))?;
            if !member.receive.is_live() {
                mark(this.ready, slot);
                *this.deferred = true;
                break;
            }
        }
        Ok(())
    }

    fn flush<'owner>(
        self: pin::Pin<&mut Self>,
        work: app::Work<'_, 'd>,
        driver: &mut retained::Context<'_, 'owner, 'd>,
    ) -> Result<(), Failure>
    where
        'd: 'owner,
    {
        let this = self.project();
        this.sender
            .flush_with(
                |slot| {
                    // SAFETY: Access checks this immutable member domain before the
                    // sender admits the slot, which remains attached to its payload.
                    &unsafe { this.members.get_unchecked(slot.index().get() as usize) }.fd
                },
                work,
                driver,
                None::<fn(_)>,
            )
            .map_err(Failure::Send)
    }

    fn fail(self: pin::Pin<&mut Self>, failure: Failure, driver: &mut retained::Context<'_, '_, 'd>)
    where
        H: Handler<'d, ID>,
    {
        let this = self.project();
        let error = match failure {
            Failure::Receive(_, error) | Failure::Send(error) => error,
        };
        this.sender.fail(error);
        for member in this.members.iter_mut() {
            pin::Pin::new(&mut member.receive).stop(driver);
        }
        if let Failure::Receive(source, error) = failure {
            this.handler.packet(
                Err(ReceiveError::Submit(error)),
                source,
                Access {
                    sender: this.sender,
                    ready: this.ready,
                    members: this.members,
                },
            );
        }
    }
}

impl<'d, ID: Identity, H: Handler<'d, ID>> executor::raw::Finishes<'d, Self>
    for Endpoint<'d, ID, H>
{
    fn call<'app, 'finish, 'owner, 'epoch>(
        finish: executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self>,
    ) -> executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self> {
        let (mut owner, install, proof) = finish.into_parts();
        let mut this = owner.as_mut().project();
        if proof.is_forced() || !this.sender.is_empty() {
            this.sender.retire().finish(|error| {
                this.handler.as_mut().sent(send::Outcome::Submit(error));
            });
        }
        for member in this.members.iter_mut() {
            pin::Pin::new(&mut member.receive).finish(proof);
        }
        executor::raw::Finish::new(owner, install, proof)
    }
}

// SAFETY: the query retains this exact group; pending sender work and every
// receive arm are included until shutdown has retired the final operation.
unsafe impl<'d, ID: Identity, H: Handler<'d, ID>> dispatch::raw::Probe<'d, Self>
    for Endpoint<'d, ID, H>
{
    type Plan = ();

    fn select() -> Option<Self::Plan> {
        Some(())
    }

    fn run(call: dispatch::raw::Call<'_, '_, '_, 'd, Self, ()>) -> schedule::Progress<'d> {
        let this = call.owner().project_ref();
        let send = this.sender.progress(call.region());
        if this.sender.is_accepting() {
            return if !this.ready.is_empty() && !*this.deferred {
                schedule::Progress::Runnable
            } else {
                send.reduce(schedule::Progress::waiting(call.region()))
            };
        }
        this.members.iter().fold(send, |progress, member| {
            progress.reduce(member.receive.progress(call.region()))
        })
    }
}

// SAFETY: the route brand and unchanged member generation identify exactly one
// receive arm in the same retained group; activation cannot replace that owner.
unsafe impl<'d, ID: Identity, H: Handler<'d, ID>> dispatch::raw::Activate<'d, ID>
    for Endpoint<'d, ID, H>
{
    type Access = dispatch::raw::Plain;

    unsafe fn activate<'turn>(
        self: pin::Pin<&mut Self>,
        target: route::Target<'d, ID>,
        _: schedule::Turn<'turn, 'd>,
        _: &mut dispatch::raw::Context<'_, '_, 'd, Self::Access>,
    ) {
        if target.epoch() != route::Epoch::INITIAL {
            return;
        }
        let this = self.project();
        let slot = target.slot();
        let Some(member) = this.members.get_mut(slot.index().get() as usize) else {
            return;
        };
        pin::Pin::new(&mut member.receive).resume();
        mark(this.ready, slot);
        *this.deferred = false;
    }
}

// SAFETY: the installed control borrows this exact pinned group for the view lifetime.
unsafe impl<'d, ID: Identity, H: Handler<'d, ID>> dispatch::raw::Control<'d>
    for Endpoint<'d, ID, H>
{
    type View<'step>
        = View<'step, 'd, ID, H>
    where
        Self: 'step,
        'd: 'step;

    fn view<'step>(owner: dispatch::raw::Installed<'step, 'd, Self>) -> Self::View<'step>
    where
        'd: 'step,
    {
        View { owner }
    }
}
