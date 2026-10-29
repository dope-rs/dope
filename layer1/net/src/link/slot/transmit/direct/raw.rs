use dope_core::driver::{
    retained::{self, flight},
    route,
    route::Identity,
};

use crate::{
    link::{
        duplex::sending,
        slot::{self, transmit::outcome},
    },
    wire::{self, send, terminal},
};

pub struct Sending<'a, 'w, 'd, ID: Identity + 'w, W: wire::Spec + 'w> {
    engine: &'a mut slot::Engine<'d, ID, W>,
    wire: &'a mut W::Connection<'w, ID>,
    store: &'a mut W::SendStorage<'w>,
    end: &'a mut terminal::State<terminal::Error<'w, W>>,
    target: route::Target<'d, ID>,
}

impl<'a, 'w, 'd, ID: Identity + 'w, W: wire::Spec + 'w> Sending<'a, 'w, 'd, ID, W> {
    pub(in crate::link::slot) fn new(
        engine: &'a mut slot::Engine<'d, ID, W>,
        wire: &'a mut W::Connection<'w, ID>,
        store: &'a mut W::SendStorage<'w>,
        end: &'a mut terminal::State<terminal::Error<'w, W>>,
        target: route::Target<'d, ID>,
    ) -> Self {
        Self {
            engine,
            wire,
            store,
            end,
            target,
        }
    }

    pub fn discard(&mut self) {
        self.engine.discard(self.end);
    }

    fn submit<'send, 'owner: 'send, B: send::Payload<'send>>(
        &'send mut self,
        slots: &flight::Slots<'d, ID>,
        driver: &mut retained::Context<'_, 'owner, 'd>,
        prepare: impl FnOnce(
            send::Context<'send, 'w, ID, W, send::Storage<'send, W::SendStorage<'w>>>,
            terminal::Send<'send, terminal::Error<'w, W>>,
        ) -> terminal::Processed<
            send::Prepared<'send, <W::Send as send::Policy<W>>::Reclaim, B>,
        >,
    ) -> outcome::Handoff {
        let end = match self.engine.send(self.end) {
            terminal::Admission::Open(end) => end,
            terminal::Admission::Closed => return outcome::Handoff::blocked(),
        };
        let Some(fd) = self.engine.establish.fd() else {
            return outcome::Handoff::blocked();
        };
        let Some(submit) = self
            .engine
            .operations
            .submission(sending::Source::Direct)
            .try_ready()
        else {
            return outcome::Handoff::blocked();
        };
        let context = send::Context::new(self.wire, send::Storage::new(self.store));
        let done = prepare(context, end);
        let (ready, captured) = done.into_parts();
        let ready = captured.resolve(ready, send::Prepared::close_after);
        let submit = submit.submit(fd, slots, driver, self.target, ready);
        self.engine.captured(captured);
        outcome::Handoff::from_attempt(submit)
    }

    /// # Safety
    /// Retain the exact immutable input backing at its submitted address until
    /// completion or quiescence, including after this call returns.
    pub unsafe fn submit_plain(
        &mut self,
        slots: &flight::Slots<'d, ID>,
        driver: &mut retained::Context<'_, '_, 'd>,
        plain: send::Plain<'_>,
    ) -> outcome::Handoff {
        self.submit(slots, driver, |context, end| {
            <W::Send as send::Policy<W>>::prepare::<ID>(context, plain, end)
        })
    }

    /// # Safety
    /// Retain the exact immutable message, iovec and payload graph at submitted
    /// addresses until completion or quiescence, including after this call.
    pub unsafe fn submit_many<const N: usize>(
        &mut self,
        plain: send::Vectored<'_, N>,
        slots: &flight::Slots<'d, ID>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> outcome::Handoff {
        self.submit(slots, driver, |context, end| {
            <W::Send as send::Policy<W>>::prepare_many::<ID, N>(context, plain, end)
        })
    }
}
