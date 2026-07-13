use super::Driver;
use crate::Sockopt;

impl Sockopt for Driver {
    fn set(
        &self,
        fixed_idx: u32,
        level: u32,
        optname: u32,
        value: i32,
    ) -> Result<(), crate::backend::PushError> {
        // SAFETY: leaf.
        unsafe { self.inner() }.set_sockopt(fixed_idx, level, optname, value)
    }
}
