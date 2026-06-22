use io_uring::{opcode::SetSockOpt, types::Fixed};

use super::Driver;
use crate::backend::token::{ROUTE_FRAMEWORK, Token};
use crate::{PushError, Sockopt};

impl Sockopt for Driver {
    fn set(&mut self, fixed_idx: u32, level: u32, optname: u32, value: i32) -> Result<(), crate::backend::PushError> {
        let Some(key) = self.setsockopt.alloc(Box::new(value)) else {
            return Err(crate::backend::PushError);
        };
        let optval_ptr =
            (&**self.setsockopt.get(key).unwrap() as *const libc::c_int).cast::<libc::c_void>();
        let ud = Token::from_key(ROUTE_FRAMEWORK, key);
        let sqe = SetSockOpt::new(
            Fixed(fixed_idx),
            level,
            optname,
            optval_ptr,
            size_of::<libc::c_int>() as u32,
        )
        .build()
        .user_data(ud.raw());
        if self.try_push(&sqe).is_ok() {
            Ok(())
        } else {
            self.setsockopt.remove(key);
            Err(PushError)
        }
    }
}