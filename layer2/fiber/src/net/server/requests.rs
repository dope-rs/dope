use dope::{
    core::driver::route::Identity,
    net::{
        self,
        link::pool::recv::{delivery, input, reserve::Choice},
        wire::{open, receive::Policy},
    },
};

use crate::{
    core::driver::schedule,
    manifold::{
        app::{dispatch, env},
        network::listener,
        timing,
    },
    net::{port::request::Requests, server::Queue},
};

pub(super) fn apply<
    'a,
    'n,
    'p,
    's,
    'scope,
    'w,
    'd: 'w,
    ID: Identity,
    T: net::Transport,
    W: input::Execution + 'd,
    F: FnMut(open::Error<'w, W>),
>(
    resume: listener::raw::Resume<
        'a,
        'n,
        'p,
        's,
        'w,
        'd,
        ID,
        Queue<'scope, 'w, 'd, W, F, ID>,
        env::Bundle<T, W, timing::Balanced>,
    >,
    reqs: Option<Requests<'_, 'd>>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut dispatch::raw::Context<'_, '_, 'd, dispatch::raw::Retained>,
) where
    <W::Receive as Policy<W>>::Reserve: Choice<W, W::Retained>,
    <W::Receive as Policy<W>>::Credit: delivery::Ownership<W>,
{
    let conn = resume.id();
    if let Some(reqs) = reqs {
        let inner = resume.listener();
        if let Some(staged) = reqs.send {
            staged.submit(|bytes| inner.mark_send(driver.region_token(), conn, bytes));
        }
        if reqs.close {
            inner.close(conn);
        }
    }
    unsafe { resume.drive(turn, driver) };
}
