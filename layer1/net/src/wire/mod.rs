use dope_core::driver::route;
pub mod domain;
pub mod failure;
pub mod open;
pub mod receive;
pub mod send;
pub mod terminal;

/// A protocol and route-storage family owned by its enclosing assembly.
/// Borrowed wire state retains its storage owner independently of the driver brand.
pub trait Spec: Sized {
    type Connection<'d, ID: route::Identity + 'd>: 'd
    where
        Self: 'd;
    type Storage<ID: route::Identity>;
    type Binding<'d, ID: route::Identity + 'd>: 'd
    where
        Self: 'd;
    type Runtime<'w, 'd, ID: route::Identity + 'w>: open::Runtime<'w, 'd, Self, ID> + 'w
    where
        Self: 'w,
        'd: 'w;
    type SendStorage<'d>: send::Store + 'd
    where
        Self: 'd;
    /// Selector that structurally determines the exact open-error family.
    type OpenFailure: failure::Class<Self, failure::Open>;
    /// Selector that structurally determines the exact terminal-error family.
    type TerminalFailure: failure::Class<Self, failure::Terminal>;

    type Bootstrap;
    type Receive: receive::Policy<Self>;
    type Send: send::Policy<Self>;
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct Identity;

/// Stateless runtime for the transparent wire.
pub struct Runtime;

impl Spec for Identity {
    type Connection<'d, ID: route::Identity + 'd>
        = Self
    where
        Self: 'd;
    type Storage<ID: route::Identity> = Self;
    type Binding<'d, ID: route::Identity + 'd>
        = Self
    where
        Self: 'd;
    type Runtime<'w, 'd, ID: route::Identity + 'w>
        = Runtime
    where
        Self: 'w,
        'd: 'w;
    type SendStorage<'d>
        = ()
    where
        Self: 'd;
    type OpenFailure = failure::Never;
    type TerminalFailure = failure::Never;

    type Bootstrap = Identity;
    type Receive = receive::Transparent;
    type Send = send::Direct;
}

impl domain::Init<Identity> for Identity {
    fn runtime<'w, 'd, ID: route::Identity + 'w>(_: domain::Limits, _: Identity) -> Runtime
    where
        Identity: 'w,
        'd: 'w,
    {
        Runtime
    }
}

impl<'w, 'd: 'w, ID: route::Identity + 'w> open::Runtime<'w, 'd, Identity, ID> for Runtime {
    fn open<C, T>(
        &mut self,
        context: C,
        consume: impl FnOnce(C, Identity, ()) -> open::Disposition<T, Identity, ()>,
    ) -> open::Outcome<C, T, open::Error<'w, Identity>> {
        let output = consume(context, Identity, ()).into_output(|wire, send| {
            let Identity = wire;
            send
        });
        open::Outcome::Opened(output)
    }
}
