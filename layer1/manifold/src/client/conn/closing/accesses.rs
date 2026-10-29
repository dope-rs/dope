use crate::client::{
    close,
    conn::{ClosePhase, CloseStatus, closing::Packed},
};

pub(in crate::client::conn) struct Quarantine<'a>(pub(in crate::client::conn) &'a mut Packed);
pub(in crate::client::conn) struct RetireState<'a>(pub(in crate::client::conn) &'a mut Packed);
pub(in crate::client::conn) struct RetireRef<'a>(pub(in crate::client::conn) &'a mut Packed);
pub(in crate::client::conn) struct SettledState<'a>(pub(in crate::client::conn) &'a Packed);

impl<'a> Quarantine<'a> {
    pub(in crate::client::conn) fn complete(self) -> RetireState<'a> {
        *self.0 = self.0.with_phase(ClosePhase::Retiring);
        RetireState(self.0)
    }
}

impl<'a> RetireState<'a> {
    pub(in crate::client::conn) fn reborrow(&mut self) -> RetireRef<'_> {
        RetireRef(&mut *self.0)
    }

    pub(in crate::client::conn) const fn reason(&self) -> close::Reason {
        self.0.effective().reason()
    }

    pub(in crate::client::conn) fn settle(self, reason: close::Reason) -> SettledState<'a> {
        let state = self.0.merged(close::Directive::reconnect(reason));
        *self.0 = state.with_phase(ClosePhase::Settled);
        SettledState(self.0)
    }
}

impl RetireRef<'_> {
    pub(in crate::client::conn) fn reborrow(&mut self) -> RetireRef<'_> {
        RetireRef(&mut *self.0)
    }

    pub(in crate::client::conn) fn request(&mut self, rule: close::Directive) {
        self.0.apply(rule);
    }

    pub(in crate::client::conn) const fn status(&self) -> CloseStatus {
        self.0.status()
    }
}

impl SettledState<'_> {
    pub(in crate::client::conn) const fn reason(&self) -> close::Reason {
        self.0.effective().reason()
    }
}
