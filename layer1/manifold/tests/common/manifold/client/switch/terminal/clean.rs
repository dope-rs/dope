use dope_core::{driver::route::Identity, io::recv};
use dope_net::wire::{
    self, domain, failure, open, receive,
    receive::{batch, eof, mode},
    send, terminal,
};
use wire::receive::{mode::access, reserve};

pub struct Clean<const EOF: bool = false>;

pub struct State {
    pub(crate) ended: bool,
}

impl State {
    fn finish<const EOF: bool, T, E>(
        &mut self,
        output: T,
        end: terminal::Both<'_, E>,
    ) -> terminal::Processed<T> {
        if self.ended {
            return end.output(output);
        }
        self.ended = true;
        if EOF {
            end.eof(output)
        } else {
            end.terminal(output, terminal::Terminal::Clean)
        }
    }
}

pub struct Ingress<const EOF: bool>;

impl<const EOF: bool> receive::Policy<Clean<EOF>> for Ingress<EOF> {
    type Batch<'a> = batch::One<&'a [u8]>;
    type Cursor<'w, 'd>
        = recv::Shared<'d>
    where
        'd: 'w;
    type Access = access::Direct;
    type Eof = eof::Clean;
    type Reserve = reserve::Borrowed;
    type Credit = mode::Disabled;
    type Discard = mode::Disabled;

    fn process<'id, 'a, 'input, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Transaction<'a, 'w, ID, Clean<EOF>>,
        bytes: &'input mut [u8],
        capacity: batch::Grant<'id, 'd, Clean<EOF>>,
        end: terminal::Both<'_, terminal::Error<'w, Clean<EOF>>>,
    ) -> terminal::Processed<batch::Produced<'id, Self::Batch<'input>>>
    where
        Clean<EOF>: 'w,
        'd: 'w,
        'w: 'a + 'input,
    {
        let output = capacity.one(&*bytes);
        txn.into_wire().finish::<EOF, _, _>(output, end)
    }

    fn read_lease<'a, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Transaction<'a, 'w, ID, Clean<EOF>>,
        bytes: recv::Lease<'d>,
        end: terminal::Both<'_, terminal::Error<'w, Clean<EOF>>>,
    ) -> terminal::Processed<Option<Self::Cursor<'w, 'd>>>
    where
        Clean<EOF>: 'w,
        'd: 'w,
        'w: 'a,
    {
        let output = Some(bytes.into_shared());
        txn.into_wire().finish::<EOF, _, _>(output, end)
    }
}

impl<'w, 'd: 'w, ID: Identity + 'w, const EOF: bool> open::Runtime<'w, 'd, Clean<EOF>, ID>
    for Clean<EOF>
{
    fn open<C, T>(
        &mut self,
        context: C,
        consume: impl FnOnce(C, State, ()) -> open::Disposition<T, State, ()>,
    ) -> open::Outcome<C, T, open::Error<'w, Clean<EOF>>> {
        let output = match consume(context, State { ended: false }, ()) {
            open::Disposition::Commit(output) | open::Disposition::Retry { output, .. } => output,
        };
        open::Outcome::Opened(output)
    }
}

impl<const EOF: bool> wire::Spec for Clean<EOF> {
    type Connection<'d, ID: Identity + 'd> = State;
    type Storage<ID: Identity> = ();
    type Binding<'d, ID: Identity + 'd> = ();
    type Runtime<'w, 'd, ID: Identity + 'w>
        = Self
    where
        Self: 'w,
        'd: 'w;
    type SendStorage<'d> = ();
    type OpenFailure = failure::Never;
    type TerminalFailure = failure::Never;

    type Bootstrap = Self;
    type Receive = Ingress<EOF>;
    type Send = send::Direct;
}

impl<const EOF: bool> domain::Init<Clean<EOF>> for Clean<EOF> {
    fn runtime<'w, 'd, ID: Identity + 'w>(
        _: domain::Limits,
        _: <Self as wire::Spec>::Binding<'w, ID>,
    ) -> <Self as wire::Spec>::Runtime<'w, 'd, ID>
    where
        Self: 'w,
        'd: 'w,
    {
        Self
    }
}
