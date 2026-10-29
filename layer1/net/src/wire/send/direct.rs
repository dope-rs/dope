use dope_core::driver::route::Identity;
use send::reclaim;

use crate::wire::{self, send, terminal};

/// Direct plaintext egress retaining input through kernel completion.
pub struct Direct;

impl<W: wire::Spec> send::Policy<W> for Direct {
    type Reclaim = reclaim::OnComplete;

    fn prepare<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, W, send::Storage<'a, W::SendStorage<'d>>>,
        plain: send::Plain<'a>,
        end: terminal::Send<'_, terminal::Error<'d, W>>,
    ) -> terminal::Processed<send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>>
    where
        'd: 'a,
    {
        let send = context.into_state();
        end.output(send.input(plain))
    }

    fn prepare_many<'a, 'd, ID: Identity + 'd, const N: usize>(
        context: send::Context<'a, 'd, ID, W, send::Storage<'a, W::SendStorage<'d>>>,
        plain: send::Vectored<'a, N>,
        end: terminal::Send<'_, terminal::Error<'d, W>>,
    ) -> terminal::Processed<send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>>
    where
        'd: 'a,
    {
        let send = context.into_state();
        end.output(send.vectored(plain))
    }

    fn after_send<'a, 'd, ID: Identity + 'd>(
        context: send::Context<
            'a,
            'd,
            ID,
            W,
            send::Completion<'a, Self::Reclaim, W::SendStorage<'d>>,
        >,
        end: terminal::Send<'_, terminal::Error<'d, W>>,
    ) -> terminal::Processed<send::Transition<'a, Self::Reclaim, impl send::Payload<'a>>>
    where
        'd: 'a,
    {
        let done = context.into_state();
        end.output(send::Transition::completed(done.into_storage()))
    }

    fn settle<'a, 'd, ID: Identity + 'd>(
        context: send::Context<
            'a,
            'd,
            ID,
            W,
            send::Completion<'a, Self::Reclaim, W::SendStorage<'d>>,
        >,
        _: Option<terminal::Drain<'_, 'd, W>>,
    ) -> send::Transition<'a, Self::Reclaim, impl send::Payload<'a>>
    where
        'd: 'a,
    {
        let done = context.into_state();
        send::Transition::completed(done.into_storage())
    }

    fn flush<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, W, send::Storage<'a, W::SendStorage<'d>>>,
        end: terminal::Send<'_, terminal::Error<'d, W>>,
    ) -> terminal::Processed<send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>>
    where
        'd: 'a,
    {
        let send = context.into_state();
        end.output(send.empty())
    }

    fn drain<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, W, send::Storage<'a, W::SendStorage<'d>>>,
        _: Option<terminal::Drain<'_, 'd, W>>,
    ) -> send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>
    where
        'd: 'a,
    {
        let send = context.into_state();
        send.empty()
    }
}
