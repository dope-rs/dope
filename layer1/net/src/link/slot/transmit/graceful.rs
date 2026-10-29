use dope_core::{
    driver::{
        retained::{self, flight},
        route,
        route::Identity,
    },
    io::fd::handles,
};

use crate::{
    link::{
        duplex::sending,
        slot::{transmit::outcome, types},
    },
    wire::{self, send, terminal},
};
pub(super) fn submit_pending<'a, 'w, 'd, ID: Identity + 'w, W: wire::Spec + 'w>(
    context: send::Context<'a, 'w, ID, W, send::Storage<'a, W::SendStorage<'w>>>,
    submit: sending::Submission<'_, 'd, <W::Send as send::Policy<W>>::Reclaim>,
    fd: &handles::Descriptor<'d>,
    slots: &flight::Slots<'d, ID>,
    driver: &mut retained::Context<'_, '_, 'd>,
    target: route::Target<'d, ID>,
    end: terminal::Admission<terminal::Send<'_, terminal::Error<'w, W>>>,
) -> (terminal::Captured, outcome::Handoff)
where
    'd: 'w,
{
    let terminal::Admission::Open(end) = end else {
        return (terminal::Captured::None, outcome::Handoff::blocked());
    };
    let sending::Pending::Ready(seal) = submit.pending() else {
        return (terminal::Captured::None, outcome::Handoff::blocked());
    };
    let done = <W::Send as send::Policy<W>>::close::<ID>(context, end);
    let (ready, captured) = done.into_parts();
    let ready = captured.resolve(ready, send::Prepared::close_after);
    let submit = seal.seal(ready.is_waiting());
    let submit = submit.submit(fd, slots, driver, target, ready);
    let handoff = outcome::Handoff::from_attempt(submit);
    (captured, handoff)
}

pub(super) fn flush<'w, 'd, ID: Identity + 'w, W, S>(
    slot: &mut types::Connection<'w, 'd, ID, W, S>,
    slots: &flight::Slots<'d, ID>,
    driver: &mut retained::Context<'_, '_, 'd>,
    target: route::Target<'d, ID>,
    source: sending::Source,
) -> outcome::Progress
where
    'd: 'w,
    W: wire::Spec + 'w,
{
    if slot
        .control
        .engine
        .operations
        .lifecycle
        .close
        .is_discarded()
    {
        return outcome::Progress::Idle;
    }
    let end = slot.control.engine.send(&mut slot.control.terminal);
    let Some(fd) = slot.control.engine.establish.fd() else {
        return outcome::Progress::Idle;
    };
    let Some(op) = slot
        .control
        .engine
        .operations
        .submission(source)
        .try_ready()
    else {
        return outcome::Progress::Idle;
    };
    let context = send::Context::new(&mut slot.wire, send::Storage::new(&mut slot.send));
    let (attempt, captured) = match end {
        terminal::Admission::Open(end) => {
            let done = <W::Send as send::Policy<W>>::flush::<ID>(context, end);
            let (ready, captured) = done.into_parts();
            let ready = captured.resolve(ready, send::Prepared::close_after);
            let attempt = op.submit(fd, slots, driver, target, ready);
            (attempt, captured)
        }
        terminal::Admission::Closed => {
            let end = slot.control.terminal.drain();
            let ready = <W::Send as send::Policy<W>>::drain::<ID>(context, end).close_after();
            let captured = terminal::Captured::None;
            let attempt = op.submit(fd, slots, driver, target, ready);
            (attempt, captured)
        }
    };
    let result = outcome::Handoff::from_attempt(attempt);
    slot.control.engine.captured(captured);
    result.progress()
}
