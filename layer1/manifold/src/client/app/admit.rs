use dope_core::driver::route::{Identity, KeyTag};

use crate::client::{app, close, conn};

pub trait Mode {}

pub struct Automatic;

impl Mode for Automatic {}

pub struct Registered;

impl Mode for Registered {}

pub struct Checked;

impl Mode for Checked {}

pub trait Register<'d, ID: Identity = KeyTag<0>>: app::Application<'d, ID> {
    fn register(&mut self, active: &conn::Activation<'_, 'd, ID>) -> app::Admission;
}

pub trait Check {
    fn admit(&mut self) -> Result<(), close::Directive>;
}

/// Statically selects one application's exact connection-admission callback.
pub trait Dispatch<'d, ID: Identity, A>: Mode
where
    A: app::Application<'d, ID>,
{
    fn admit(app: &mut A, active: &conn::Activation<'_, 'd, ID>) -> app::Admission;
}

impl<'d, ID: Identity, A> Dispatch<'d, ID, A> for Automatic
where
    A: app::Application<'d, ID>,
{
    fn admit(_: &mut A, _: &conn::Activation<'_, 'd, ID>) -> app::Admission {
        app::Admission::Attached
    }
}

impl<'d, ID: Identity, A> Dispatch<'d, ID, A> for Registered
where
    A: Register<'d, ID>,
{
    fn admit(app: &mut A, active: &conn::Activation<'_, 'd, ID>) -> app::Admission {
        <A as Register<'d, ID>>::register(app, active)
    }
}

impl<'d, ID: Identity, A> Dispatch<'d, ID, A> for Checked
where
    A: app::Application<'d, ID> + Check,
{
    fn admit(app: &mut A, _: &conn::Activation<'_, 'd, ID>) -> app::Admission {
        match Check::admit(app) {
            Ok(()) => app::Admission::Attached,
            Err(rule) => app::Admission::Rejected(rule),
        }
    }
}
