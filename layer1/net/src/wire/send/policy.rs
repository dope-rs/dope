use dope_core::driver::route::Identity;
use send::reclaim;

use crate::wire::{self, send, terminal};

/// Plaintext transformation and completion policy for one wire layout.
pub trait Policy<W: wire::Spec> {
    type Reclaim: reclaim::Policy;

    /// Whether a connection waiting for shared send storage may retry.
    /// Called only while the pool owns a parked sender.
    fn has_capacity<'w, 'd: 'w, ID: Identity + 'w>(_: &W::Runtime<'w, 'd, ID>) -> bool
    where
        W: 'w,
    {
        true
    }

    fn prepare<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, W, send::Storage<'a, W::SendStorage<'d>>>,
        plain: send::Plain<'a>,
        end: terminal::Send<'_, terminal::Error<'d, W>>,
    ) -> terminal::Processed<send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>>
    where
        W: 'd,
        'd: 'a;

    fn prepare_many<'a, 'd, ID: Identity + 'd, const N: usize>(
        context: send::Context<'a, 'd, ID, W, send::Storage<'a, W::SendStorage<'d>>>,
        plain: send::Vectored<'a, N>,
        end: terminal::Send<'_, terminal::Error<'d, W>>,
    ) -> terminal::Processed<send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>>
    where
        W: 'd,
        'd: 'a;

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
        W: 'd,
        'd: 'a;

    /// Completes output after terminal admission has already been consumed.
    fn settle<'a, 'd, ID: Identity + 'd>(
        context: send::Context<
            'a,
            'd,
            ID,
            W,
            send::Completion<'a, Self::Reclaim, W::SendStorage<'d>>,
        >,
        end: Option<terminal::Drain<'_, 'd, W>>,
    ) -> send::Transition<'a, Self::Reclaim, impl send::Payload<'a>>
    where
        W: 'd,
        'd: 'a;

    fn flush<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, W, send::Storage<'a, W::SendStorage<'d>>>,
        end: terminal::Send<'_, terminal::Error<'d, W>>,
    ) -> terminal::Processed<send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>>
    where
        W: 'd,
        'd: 'a;

    /// Drains output after terminal admission has already been consumed.
    fn drain<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, W, send::Storage<'a, W::SendStorage<'d>>>,
        end: Option<terminal::Drain<'_, 'd, W>>,
    ) -> send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>
    where
        W: 'd,
        'd: 'a;

    fn close<'a, 'd, ID: Identity + 'd>(
        context: send::Context<'a, 'd, ID, W, send::Storage<'a, W::SendStorage<'d>>>,
        end: terminal::Send<'_, terminal::Error<'d, W>>,
    ) -> terminal::Processed<send::Prepared<'a, Self::Reclaim, impl send::Payload<'a>>>
    where
        W: 'd,
        'd: 'a,
    {
        let send = context.into_state();
        end.output(send.empty())
    }
}
