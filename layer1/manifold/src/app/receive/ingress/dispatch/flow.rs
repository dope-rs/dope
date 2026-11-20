use dope_core::driver::{self, route::Identity, schedule};
use dope_net::link::event;

use crate::app::receive::{Outcome, ingress::{self, Policy}};

pub(in crate::app::receive::ingress) fn apply_done<
    'ctx,
    'input,
    'a,
    'p,
    'w: 'input,
    'd: 'w + 'p,
    ID: Identity + 'd,
    P,
>(
    context: &mut P::Context<'ctx>,
    entry: ingress::Entry<'a, 'p, 'w, 'd, ID, P>,
    next: event::DispatchRecv<'d, ID, crate::Incoming<'input, 'w, 'd, P::Wire, P::Input>>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
) -> Option<ingress::Entry<'a, 'p, 'w, 'd, ID, P>>
where
    P: Policy<'p, 'w, 'd, ID> + 'ctx,
    crate::Ingress<P::Wire, P::Input>:
        ingress::Strategy<P::Wire, P::Input, Completion = Outcome>,
{
    match next {
        event::DispatchRecv::Control(next) => {
            ingress::apply_control::<ID, P>(context, entry, next, turn, driver)
        }
        event::DispatchRecv::Chunk(_, chunk) => {
            ingress::prepare_terminal::<ID, P>(&entry);
            let (entry, done) = P::receive(context, entry, chunk, turn.reborrow(), driver);
            ingress::finish_chunk::<ID, P>(context, entry?, done, turn, driver)
        }
    }
}
