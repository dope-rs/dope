use crate::backend::PushError;

pub trait Sockopt {
    fn set(&self, fixed_idx: u32, level: u32, optname: u32, value: i32) -> Result<(), PushError>;
}
