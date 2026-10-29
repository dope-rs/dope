use std::mem;

use drive::turn::scope::Core;
use o3::base::cell::brand;

use crate::executor::{self, drive};

pub(crate) trait Mode<'d>: Sized {
    fn park(core: &mut Core<'_, 'd, Self>);
}

pub(crate) struct App<'turn, 'app, 'd: 'app, D: executor::raw::Layout<'d>, S> {
    pub(super) view: executor::raw::View<'app, 'd, D, S, D::ParkPlan>,
    pub(super) token: &'turn mut brand::Token<brand::Identity<'d>>,
}

#[derive(Clone, Copy)]
pub(crate) struct Boot;

impl<'app, 'd: 'app, D, S> Mode<'d> for App<'_, 'app, 'd, D, S>
where
    D: executor::raw::App<'d>,
{
    fn park(core: &mut Core<'_, 'd, Self>) {
        core.turn.expire();
        core.mode
            .view
            .pre_park(core.mode.token, core.turn.turn(), &mut core.driver);
    }
}

impl<'d> Mode<'d> for Boot {
    fn park(core: &mut Core<'_, 'd, Self>) {
        core.turn.expire();
    }
}

const _: () = assert!(mem::size_of::<Boot>() == 0);
