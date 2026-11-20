use std::{ops::ControlFlow, pin};

use dope_core::driver::{self, route, route::Identity, schedule};
use dope_net::{
    link::{
        egress::data::{self, compose, exec},
        event::ParkError,
        pool::{
            self,
            recv::{input, reserve::Choice},
        },
    },
    wire::terminal,
};
use handler::{failures, policy::Policy};
use o3::base::cell::region;

use crate::{
    app::{
        env,
        receive::{self, ingress},
    },
    network::listener::{
        self, conn, handler,
        runtime::control,
        writer::{self, memory, phase},
    },
};

pub(crate) struct Context<'a, 'p, 's, 'd, ID: Identity, A: handler::Application<'d, ID>> {
    closing: writer::Closing<'a, 'a, 'a, 'p, 'd, ID, A>,
    writes: &'p memory::Arena<'s, 'd, ID, A::Egress>,
    authority: &'a mut <A::Input as receive::Resources>::Authority<'d>,
}

impl<'n, 'p, 's, 'w, 'd: 'w, ID: Identity + 'd, A, E> ingress::Policy<'n, 'w, 'd, ID>
    for listener::Listener<'n, 'p, 's, 'w, 'd, ID, A, E>
where
    A: handler::Application<'d, ID>,
    A::WireFailure: failures::Policy<'w, 'd, A, ID>,
    A::Input: Policy<'w, 'd, ID, A>,
    <A::Input as input::Mode<A::Wire>>::Reserve:
        Choice<A::Wire, <A::Input as input::Mode<A::Wire>>::Ingress>,
    E: env::Spec<Wire = A::Wire>,
{
    type Transport = E::Transport;
    type Wire = A::Wire;
    type State = conn::State<'p, 's, 'd, ID, A::Conn, A::Egress>;
    type Payload = compose::Either<listener::raw::Owned<'p, 's, 'd, ID>, A::Egress>;
    type Reserve = <<A::Egress as data::raw::Payload<'d>>::Mode as exec::Mode>::Reserve<
        { memory::WRITE_BUF_CAP },
    >;
    type Input = A::Input;

    type Context<'a> = Context<'a, 'p, 's, 'd, ID, A> where Self: 'a;

    fn parts<'a>(
        self: pin::Pin<&'a mut Self>,
    ) -> (&'a mut crate::Inbound<'n, 'w, 'd, ID, Self>, Self::Context<'a>) {
        let this = self.project();
        let writer::Owner { pool, writes, deadlines } = this.owner;
        (pool, Context {
            closing: (this.app, this.accept, deadlines),
            writes: *writes,
            authority: this.receive_authority,
        })
    }

    fn receipt<'ctx>(
        _: &mut Self::Context<'ctx>,
        _: pool::Key<'d, ID>,
        _: &Self::State,
        _: &region::Token<region::Identity<'d>>,
    ) -> <Self::Input as input::Mode<Self::Wire>>::Receipt
    where
        Self: 'ctx,
    {
        Default::default()
    }

    fn admit(
        self: pin::Pin<&mut Self>,
        target: impl FnOnce() -> Option<route::Target<'d, ID>>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> ControlFlow<schedule::ready::Target<'d>> {
        control::admit_deadline(self, target, turn, driver)
    }

    fn receive<'ctx, 'a, 'input>(
        context: &mut Self::Context<'ctx>,
        mut entry: ingress::Entry<'a, 'n, 'w, 'd, ID, Self>,
        input: crate::Incoming<'input, 'w, 'd, Self::Wire, Self::Input>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> (Option<ingress::Entry<'a, 'n, 'w, 'd, ID, Self>>, crate::Receiving<Self::Wire, Self::Input>)
    where
        Self: 'ctx,
    {
        let Context { closing: (app, _, deadlines), writes, authority } = context;
        let progress = match entry.egress_mut() {
            Some(view) => {
                let mut egress = writer::Egress::<ID, A>::new(view, *writes, deadlines);
                <A::Input as Policy<'w, 'd, ID, A>>::receive(
                    app.as_mut(), &mut egress, authority, input, turn, driver,
                )
            }
            None => receive::Outcome::Continue.into(),
        };
        (Some(entry), progress)
    }

    fn finish<'ctx, 'a>(
        context: &mut Self::Context<'ctx>,
        mut entry: ingress::Entry<'a, 'n, 'w, 'd, ID, Self>,
        finish: ingress::Finish,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Option<ingress::Entry<'a, 'n, 'w, 'd, ID, Self>>
    where
        Self: 'ctx,
    {
        let wake = match finish {
            ingress::Finish::Empty(ingress::Empty::Discarded { finished }) => {
                if finished {
                    let (app, _, deadlines) = &mut context.closing;
                    let Some(view) = entry.egress_mut() else { return Some(entry) };
                    let mut egress = writer::Egress::<ID, A>::new(view, context.writes, deadlines);
                    A::discarded(app.as_mut(), egress.context(turn.reborrow().app()), driver);
                }
                phase::Wake::Progress
            }
            ingress::Finish::Empty(ingress::Empty::NoChunk) | ingress::Finish::Chunk => {
                phase::Wake::Io
            }
            ingress::Finish::CloseAfter => {
                return if phase::current::<ID, A>(entry.get(), entry.egress(), context.closing.0.as_ref())
                    .is_some_and(|action| action.apply::<ID, A, _>(&mut entry))
                {
                    writer::close(Ok(entry), context.writes, &mut context.closing, turn, driver)
                } else {
                    Some(entry)
                };
            }
        };
        phase::advance_entry(entry, context.writes, &mut context.closing, wake, turn, driver)
    }

    fn terminal<'ctx, 'a>(
        context: &mut Self::Context<'ctx>,
        mut entry: ingress::Entry<'a, 'n, 'w, 'd, ID, Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> (Option<ingress::Entry<'a, 'n, 'w, 'd, ID, Self>>, ingress::Terminal)
    where
        Self: 'ctx,
    {
        {
            let (app, _, deadlines) = &mut context.closing;
            let Some(view) = entry.egress_mut() else {
                return (Some(entry), ingress::Terminal::Idle);
            };
            let mut egress = writer::Egress::<ID, A>::new(view, context.writes, deadlines);
            let cause = match egress.connection.terminal.finish_recv() {
                terminal::Receive::Idle => return (Some(entry), ingress::Terminal::Idle),
                terminal::Receive::Pending => return (Some(entry), ingress::Terminal::Pending),
                terminal::Receive::Eof => {
                    return (Some(entry), ingress::Terminal::Ended(receive::End::Read));
                }
                terminal::Receive::Ready(terminal::Terminal::Clean) => {
                    return (Some(entry), ingress::Terminal::Ended(receive::End::Both));
                }
                terminal::Receive::Ready(terminal::Terminal::Failed(cause)) => cause,
            };
            <A::WireFailure as failures::Policy<'w, 'd, A, ID>>::wire_failed(
                app.as_mut(), egress.context(turn.reborrow().app()), cause,
            );
        }
        (
            writer::close(Ok(entry), context.writes, &mut context.closing, turn, driver),
            ingress::Terminal::Closed,
        )
    }

    fn eof<'ctx, 'a>(
        context: &mut Self::Context<'ctx>,
        mut entry: ingress::Entry<'a, 'n, 'w, 'd, ID, Self>,
        end: receive::End,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Option<ingress::Entry<'a, 'n, 'w, 'd, ID, Self>>
    where
        Self: 'ctx,
    {
        {
            let (app, _, deadlines) = &mut context.closing;
            let Some(view) = entry.egress_mut() else { return Some(entry) };
            let mut egress = writer::Egress::<ID, A>::new(view, context.writes, deadlines);
            egress.connection.end_discard();
            let mut conn = egress.context(turn.reborrow().app());
            conn.close_after();
            A::eof(app.as_mut(), conn, end, driver);
        }
        phase::advance_entry(
            entry, context.writes, &mut context.closing, phase::Wake::Io, turn, driver,
        )
    }

    fn close<'ctx, 'a>(
        context: &mut Self::Context<'ctx>,
        mut entry: ingress::Entry<'a, 'n, 'w, 'd, ID, Self>,
        cause: ingress::CloseCause,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Option<ingress::Entry<'a, 'n, 'w, 'd, ID, Self>>
    where
        Self: 'ctx,
    {
        let slot = entry.get_mut();
        match cause {
            ingress::CloseCause::Io(error) => slot.state.fail(conn::Failure::Receive(error)),
            ingress::CloseCause::Receive(ParkError::Submit(error)) => {
                slot.state.fail(conn::Failure::Submit(error));
            }
            ingress::CloseCause::Receive(error) => slot.state.fail(conn::Failure::Park(error)),
            ingress::CloseCause::Application(error) => slot.state.fail(conn::Failure::Application(error)),
            ingress::CloseCause::Local | ingress::CloseCause::Transport => {}
        }
        writer::close(Ok(entry), context.writes, &mut context.closing, turn, driver)
    }
}
