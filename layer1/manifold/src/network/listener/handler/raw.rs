use std::{ops::ControlFlow, pin};

use dope_core::driver::{route, route::Identity, schedule};

use crate::{
    app::{dispatch, env},
    network::listener::{
        self,
        handler::{self, Runtime},
        runtime::control,
        writer::phase,
    },
};

/// # Safety
/// `listener` must remain installed for `driver` through this call.
pub(in crate::network::listener) unsafe fn activate<'turn, 'w, 'd: 'w, ID: Identity, A, E>(
    mut listener: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
    target: route::Target<'d, ID>,
    turn: schedule::Turn<'turn, 'd>,
    driver: &mut dispatch::raw::Context<'_, '_, 'd, dispatch::raw::Retained>,
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
    let key = {
        let fields = listener.as_ref().project_ref();
        let view = fields.owner.pool().inspect();
        let Some(key) = view.by_target(target) else {
            return;
        };
        key
    };
    {
        let mut fields = listener.as_mut().project();
        let Some(mut egress) = fields.owner.egress_mut(key) else {
            return;
        };
        handler::Application::activate(
            fields.app.as_mut(),
            egress.context(turn.reborrow().app()),
            driver,
        );
    }
    phase::close_ready(listener.as_mut(), key, turn.reborrow(), driver);
    phase::flush_dirty(listener.as_mut(), turn, driver);
}
