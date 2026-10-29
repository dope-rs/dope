use std::{convert::Infallible, pin};

use dope_core::{driver::route::Identity, io::socket};
use dope_net::wire::{self, failure, open, terminal};
use o3::base::Call;

use crate::network::listener::{conn, handler};

/// Consumes exact failures for the original wire owner.
pub trait Policy<'w, 'd: 'w, A: handler::Application<'d, ID>, ID: Identity> {
    fn open_failed(app: pin::Pin<&mut A>, cause: open::Error<'w, A::Wire>);

    /// Optionally observes an accepted socket whose peer address is invalid.
    fn peer() -> Option<impl for<'a> Call<(pin::Pin<&'a mut A>, socket::Error)>>;

    fn wire_failed<'e>(
        app: pin::Pin<&mut A>,
        conn: conn::Ctx<'e, '_, '_, '_, 'w, 'd, ID, A::Wire, A::Conn, A::Egress>,
        cause: terminal::Error<'w, A::Wire>,
    );
}

/// Eliminates failures excluded by the wire's type.
pub struct Close;

impl<'w, 'd: 'w, A, ID: Identity> Policy<'w, 'd, A, ID> for Close
where
    A: handler::Application<'d, ID>,
    A::Wire: wire::Spec<OpenFailure = failure::Never, TerminalFailure = failure::Never>,
{
    fn open_failed(_: pin::Pin<&mut A>, cause: open::Error<'w, A::Wire>) {
        cause.eliminate()
    }

    fn peer() -> Option<impl for<'a> Call<(pin::Pin<&'a mut A>, socket::Error)>> {
        None::<Infallible>
    }

    fn wire_failed<'e>(
        _: pin::Pin<&mut A>,
        _: conn::Ctx<'e, '_, '_, '_, 'w, 'd, ID, A::Wire, A::Conn, A::Egress>,
        cause: terminal::Error<'w, A::Wire>,
    ) {
        cause.eliminate()
    }
}
