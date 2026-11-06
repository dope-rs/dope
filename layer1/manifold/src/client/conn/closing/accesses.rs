use crate::client::{
    close,
    conn::{CloseStatus, closing::Packed},
};

pub(in crate::client::conn) struct RetireRef<'a>(pub(in crate::client::conn) &'a mut Packed);

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
