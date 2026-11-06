use dope_core::driver::{self, retained::flight, route::Identity};

use crate::{
    link::{
        duplex::sending,
        slot::{transmit::outcome, types},
    },
    wire::{self, send, terminal},
};

fn submit<
    'send,
    'w,
    'd: 'w + 'send,
    ID: Identity + 'w,
    W: wire::Spec + 'w,
    B: send::Payload<'send>,
>(
    slot: &'send mut types::Control<'w, 'd, ID, W>,
    slots: &flight::Slots<'d, ID>,
    driver: &mut driver::Context<'_, 'd>,
    prepare: impl FnOnce(
        send::Context<'send, 'w, ID, W, send::Storage<'send, W::SendStorage<'w>>>,
        terminal::Send<'send, terminal::Error<'w, W>>,
    ) -> terminal::Processed<
        send::Prepared<'send, <W::Send as send::Policy<W>>::Reclaim, B>,
    >,
) -> outcome::Handoff {
    let end = match slot.engine.send(&mut slot.terminal) {
        terminal::Admission::Open(end) => end,
        terminal::Admission::Closed => return outcome::Handoff::blocked(),
    };
    let Some(fd) = slot.engine.establish.fd() else {
        return outcome::Handoff::blocked();
    };
    let Some(submit) = slot
        .engine
        .operations
        .submission(sending::Source::Direct)
        .try_ready()
    else {
        return outcome::Handoff::blocked();
    };
    let context = send::Context::new(&mut slot.wire, send::Storage::new(&mut slot.send));
    let done = prepare(context, end);
    let (ready, captured) = done.into_parts();
    let ready = ready.resolve(captured);
    let submit = submit.submit(fd, slots, driver, slot.key.target(), ready);
    slot.engine.captured(captured);
    outcome::Handoff::from_attempt(submit)
}

/// # Safety
/// Retain the exact immutable input backing at its submitted address until
/// completion or quiescence, including after this call returns.
pub unsafe fn submit_plain<'w, 'd: 'w, ID: Identity + 'w, W: wire::Spec + 'w>(
    slot: &mut types::Control<'w, 'd, ID, W>,
    slots: &flight::Slots<'d, ID>,
    driver: &mut driver::Context<'_, 'd>,
    plain: send::Plain<'_>,
) -> outcome::Handoff {
    submit(slot, slots, driver, |context, end| {
        <W::Send as send::Policy<W>>::prepare::<ID>(context, plain, end)
    })
}

/// # Safety
/// Retain the exact immutable message, iovec and payload graph at submitted
/// addresses until completion or quiescence, including after this call.
pub unsafe fn submit_many<'w, 'd: 'w, ID: Identity + 'w, W: wire::Spec + 'w, const N: usize>(
    slot: &mut types::Control<'w, 'd, ID, W>,
    plain: send::Vectored<'_, N>,
    slots: &flight::Slots<'d, ID>,
    driver: &mut driver::Context<'_, 'd>,
) -> outcome::Handoff {
    submit(slot, slots, driver, |context, end| {
        <W::Send as send::Policy<W>>::prepare_many::<ID, N>(context, plain, end)
    })
}
