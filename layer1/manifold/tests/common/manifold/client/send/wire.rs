use dope_core::{
    driver::route::Identity,
    io::recv::{Lease, Shared},
};
use dope_net::wire::{
    self, domain, failure, open, receive,
    receive::{batch, eof, mode},
    send,
    send::reclaim,
    terminal,
};
use wire::receive::{mode::access, reserve};

#[derive(Default)]
pub struct Wire {
    pub(crate) established: bool,
}

pub struct Recv;

pub struct Runtime;

impl<'w, 'd: 'w, ID: Identity + 'w> open::Runtime<'w, 'd, Wire, ID> for Runtime {
    fn open<C, T>(
        &mut self,
        context: C,
        consume: impl FnOnce(C, Wire, ()) -> open::Disposition<T, Wire, ()>,
    ) -> open::Outcome<C, T, open::Error<'w, Wire>> {
        let output = match consume(context, Wire::default(), ()) {
            open::Disposition::Commit(output) | open::Disposition::Retry { output, .. } => output,
        };
        open::Outcome::Opened(output)
    }
}

impl receive::Policy<Wire> for Recv {
    type Batch<'a> = batch::One<&'a [u8]>;
    type Cursor<'w, 'd>
        = Shared<'d>
    where
        'd: 'w;
    type Access = access::Direct;
    type Eof = eof::Clean;
    type Reserve = reserve::Borrowed;
    type Credit = mode::Disabled;
    type Discard = mode::Enabled;

    fn process<'id, 'a, 'input, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Transaction<'a, 'w, ID, Wire>,
        bytes: &'input mut [u8],
        capacity: batch::Grant<'id, 'd, Wire>,
        end: terminal::Both<'_, terminal::Error<'w, Wire>>,
    ) -> terminal::Processed<batch::Produced<'id, Self::Batch<'input>>>
    where
        Wire: 'w,
        'd: 'w,
        'w: 'a + 'input,
    {
        let wire = txn.into_wire();
        wire.established = true;
        let output = capacity.maybe(Some(&*bytes));
        end.output(output)
    }

    fn read_lease<'a, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Transaction<'a, 'w, ID, Wire>,
        bytes: Lease<'d>,
        end: terminal::Both<'_, terminal::Error<'w, Wire>>,
    ) -> terminal::Processed<Option<Self::Cursor<'w, 'd>>>
    where
        Wire: 'w,
        'd: 'w,
        'w: 'a,
    {
        let wire = txn.into_wire();
        wire.established = true;
        let output = Some(bytes.into_shared());
        end.output(output)
    }
}

pub struct Write;

impl send::Policy<Wire> for Write {
    type Reclaim = reclaim::OnComplete;

    fn prepare<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, Wire, send::Storage<'a, ()>>,
        plain: send::Plain<'a>,
        end: terminal::Send<'_, terminal::Error<'d, Wire>>,
    ) -> terminal::Processed<send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>>
    where
        'd: 'a,
    {
        let (wire, send) = context.into_parts();
        if wire.established {
            end.output(send.input(plain).into_right::<send::Empty>())
        } else {
            end.output(
                send.empty::<reclaim::OnComplete>()
                    .into_left::<send::Plain<'a>>(),
            )
        }
    }

    fn prepare_many<'a, 'd, ID: Identity + 'd, const N: usize>(
        context: send::Context<'a, 'd, ID, Wire, send::Storage<'a, ()>>,
        plain: send::Vectored<'a, N>,
        end: terminal::Send<'_, terminal::Error<'d, Wire>>,
    ) -> terminal::Processed<send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>>
    where
        'd: 'a,
    {
        let (wire, send) = context.into_parts();
        if wire.established {
            end.output(send.vectored(plain).into_right::<send::Empty>())
        } else {
            end.output(
                send.empty::<reclaim::OnComplete>()
                    .into_left::<send::Vectored<'a, N>>(),
            )
        }
    }

    fn after_send<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, Wire, send::Completion<'a, reclaim::OnComplete, ()>>,
        end: terminal::Send<'_, terminal::Error<'d, Wire>>,
    ) -> terminal::Processed<send::Transition<'a, Self::Reclaim, impl send::Payload<'a>>>
    where
        'd: 'a,
    {
        let done = context.into_state();
        end.output(send::Transition::completed(done.into_storage()))
    }

    fn settle<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, Wire, send::Completion<'a, reclaim::OnComplete, ()>>,
        _: Option<terminal::Drain<'_, 'd, Wire>>,
    ) -> send::Transition<'a, Self::Reclaim, impl send::Payload<'a>>
    where
        'd: 'a,
    {
        let done = context.into_state();
        send::Transition::completed(done.into_storage())
    }

    fn flush<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, Wire, send::Storage<'a, ()>>,
        end: terminal::Send<'_, terminal::Error<'d, Wire>>,
    ) -> terminal::Processed<send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>>
    where
        'd: 'a,
    {
        let send = context.into_state();
        end.output(send.empty())
    }

    fn drain<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, Wire, send::Storage<'a, ()>>,
        _: Option<terminal::Drain<'_, 'd, Wire>>,
    ) -> send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>
    where
        'd: 'a,
    {
        let send = context.into_state();
        send.empty()
    }
}

impl wire::Spec for Wire {
    type Connection<'d, ID: Identity + 'd> = Self;
    type Storage<ID: Identity> = ();
    type Binding<'d, ID: Identity + 'd> = ();
    type Runtime<'w, 'd, ID: Identity + 'w>
        = Runtime
    where
        Self: 'w,
        'd: 'w;
    type SendStorage<'d> = ();
    type OpenFailure = failure::Never;
    type TerminalFailure = failure::Never;
    type Bootstrap = Wire;
    type Receive = Recv;
    type Send = Write;
}

impl domain::Init<Wire> for Wire {
    fn runtime<'w, 'd, ID: Identity + 'w>(
        _: domain::Limits,
        _: <Wire as wire::Spec>::Binding<'w, ID>,
    ) -> <Wire as wire::Spec>::Runtime<'w, 'd, ID>
    where
        Wire: 'w,
        'd: 'w,
    {
        Runtime
    }
}
