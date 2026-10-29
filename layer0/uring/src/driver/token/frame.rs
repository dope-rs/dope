use std::num;

use dope_sys::table::token::{self, Tuning as _, frame};
use o3::base::num::bounded;

use crate::driver::token::{PREP, TUNING, Tuning};

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Frame<C: token::Context>(C::Control);

impl<C: token::Context> frame::Decode for Frame<C> {
    type Context = C;
    type Tuning = Tuning<C::Slot>;

    fn from_raw(raw: u64) -> Result<Self, u64> {
        Ok(Self(C::Control::try_from(raw)?))
    }

    fn decode(self) -> frame::Control<Self::Tuning> {
        if self.0 == C::SHUTDOWN {
            return frame::Control::Shutdown;
        }
        let raw: num::NonZero<u64> = self.0.into();
        let slot = C::Slot::from(bounded::U32::clamp((raw.get() & super::SLOT) as usize));
        if self.0 == C::control(TUNING, slot) {
            return frame::Control::Tuning(Tuning::new(slot));
        }
        if self.0 == C::control(PREP, C::ZERO) {
            return frame::Control::ClosePrep;
        }
        frame::Control::Unknown
    }

    fn control(self) -> C::Control {
        self.0
    }
}
