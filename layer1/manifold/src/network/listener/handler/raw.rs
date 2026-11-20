use std::{ops::ControlFlow, pin};

use dope_core::driver::{route, route::Identity, schedule};

use crate::{
    app::{dispatch, env},
    network::listener::{
        self,
        handler::{self, Runtime},
        runtime::control,
        writer::{self, phase},
    },
};

/// # Safety
/// `listener` must remain installed for `driver` through this call.
pub(in crate::network::listener) unsafe fn activate<'turn, 'w, 'd: 'w, ID: Identity, A, E>(
    mut listener: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
    target: route::Target<'d, ID>,
    turn: schedule::Turn<'turn, 'd>,
    driver: &mut dispatch::raw::Context<'_, 'd, dispatch::raw::Retained>,
) where
    A: Runtime<'w, 'd, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    <A as Runtime<'w, 'd, ID>>::resume(listener.as_mut(), target, turn.reborrow(), driver);
    if let ControlFlow::Break(wake) =
        control::admit_deadline(listener.as_mut(), || Some(target), turn.reborrow(), driver)
    {
        wake.wake();
        return;
    }
    {
        let mut fields = listener.as_mut().project();
        let writer::Owner { pool, deadlines, writes } = fields.owner;
        let Some(mut entry) = pool.by_target(target) else {
            return;
        };
        let Some(view) = entry.egress_mut() else { return };
        handler::Application::activate(
            fields.app.as_mut(),
            writer::Egress::<ID, A>::new(view, *writes, deadlines).context(turn.reborrow().app()),
            driver,
        );
        if phase::current::<ID, A>(entry.get(), entry.egress(), fields.app.as_ref())
            .is_some_and(|action| action.apply::<ID, A, _>(&mut entry))
        {
            drop(writer::close(
                Ok(entry), *writes, &mut (fields.app, fields.accept, deadlines), turn.reborrow(), driver,
            ));
        }
    }
    phase::flush_dirty(listener.as_mut(), turn, driver);
}
