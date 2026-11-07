use dope_core::driver::route::Identity;

use crate::wire::{
    self,
    receive::{Policy, mode, reserve},
    send::Availability,
};

/// Delivers returned send capacity under the exact receive reservation policy.
pub trait Finish<W: wire::Spec>: Sized {
    fn release<'w, 'd: 'w, ID: Identity + 'w>(_: Availability, _: &mut W::Runtime<'w, 'd, ID>)
    where
        W: 'w + 'd,
    {
    }
}

impl<W> Finish<W> for mode::Disabled
where
    W: wire::Spec,
    <W::Receive as Policy<W>>::Reserve: reserve::Immediate<W>,
{
}

impl<W> Finish<W> for mode::Parking
where
    W: wire::Spec,
    <W::Receive as Policy<W>>::Reserve: reserve::Immediate<W>,
{
}

impl<W> Finish<W> for mode::Enabled
where
    W: wire::Spec,
    <W::Receive as Policy<W>>::Reserve: reserve::Immediate<W>,
{
}

impl<W, P> Finish<W> for mode::Waiting<P>
where
    W: wire::Spec,
    P: reserve::Try<W>,
    W::Receive: Policy<W, Reserve = reserve::Waiting<P>>,
{
    fn release<'w, 'd: 'w, ID: Identity + 'w>(space: Availability, run: &mut W::Runtime<'w, 'd, ID>)
    where
        W: 'w + 'd,
    {
        if space == Availability::Released {
            P::release_send::<ID>(run);
        }
    }
}
