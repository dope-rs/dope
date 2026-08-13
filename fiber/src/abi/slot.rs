use core::pin;
use std::process;

#[pin_project::pin_project(project = SlotProj, project_replace = SlotProjOwn)]
pub(crate) enum Slot<F, O> {
    Vacant,
    Live(#[pin] F),
    Done(O),
}

impl<F, O> Slot<F, O> {
    pub(crate) const fn vacant() -> Self {
        Self::Vacant
    }

    pub(crate) const fn live(fiber: F) -> Self {
        Self::Live(fiber)
    }

    pub(crate) fn write(&mut self, fiber: F) {
        debug_assert!(matches!(self, Self::Vacant));
        *self = Self::Live(fiber);
    }

    pub(crate) fn fill(self: pin::Pin<&mut Self>, fiber: F) -> Result<(), F> {
        if !self.is_vacant() {
            return Err(fiber);
        }
        match self.project_replace(Self::Live(fiber)) {
            SlotProjOwn::Vacant => Ok(()),
            SlotProjOwn::Live(_) | SlotProjOwn::Done(_) => {
                use std::process::abort;
                abort();
            }
        }
    }

    pub(crate) fn as_live(self: pin::Pin<&mut Self>) -> Option<pin::Pin<&mut F>> {
        match self.project() {
            SlotProj::Live(fiber) => Some(fiber),
            SlotProj::Vacant | SlotProj::Done(_) => None,
        }
    }

    pub(crate) fn complete(self: pin::Pin<&mut Self>, output: O) {
        match self.project_replace(Self::Done(output)) {
            SlotProjOwn::Live(_) => {}
            SlotProjOwn::Vacant | SlotProjOwn::Done(_) => process::abort(),
        }
    }

    pub(crate) fn cancel(self: pin::Pin<&mut Self>) {
        match self.project_replace(Self::Vacant) {
            SlotProjOwn::Live(_) => {}
            SlotProjOwn::Vacant | SlotProjOwn::Done(_) => process::abort(),
        }
    }

    pub(crate) fn take_output(self: pin::Pin<&mut Self>) -> O {
        match self.project_replace(Self::Vacant) {
            SlotProjOwn::Done(output) => output,
            SlotProjOwn::Vacant | SlotProjOwn::Live(_) => process::abort(),
        }
    }

    pub(crate) fn is_live(&self) -> bool {
        matches!(self, Self::Live(_))
    }

    pub(crate) fn is_done(&self) -> bool {
        matches!(self, Self::Done(_))
    }

    pub(crate) fn is_vacant(&self) -> bool {
        matches!(self, Self::Vacant)
    }
}
