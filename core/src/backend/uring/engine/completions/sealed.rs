use std::os::fd;

use crate::driver::{flight, route};

pub(in crate::backend::uring::engine) struct Decoder(u64);

pub(in crate::backend::uring::engine) struct Opened(i32);

impl Decoder {
    pub(super) const fn new(raw: u64) -> Self {
        Self(raw)
    }

    pub(super) fn decode<'q, 'd>(
        self,
        drain: &'q flight::Drain<'q, 'd>,
    ) -> super::UserData<'q, 'd> {
        let framework = route::Token::try_from_framework_raw(self.0)
            .filter(|token| token.route() == route::FRAMEWORK);
        match framework {
            Some(token) => super::UserData::Framework(token),
            None => match unsafe { flight::raw::Echo::from_kernel(self.0) } {
                Some(key) => drain
                    .complete(key)
                    .map_or(super::UserData::Empty, super::UserData::Flight),
                None => super::UserData::Empty,
            },
        }
    }
}

impl Opened {
    pub(super) const fn new(raw: i32) -> Self {
        Self(raw)
    }

    pub(super) fn into_fd(self) -> Result<fd::OwnedFd, i32> {
        if self.0 < 0 {
            return Err(self
                .0
                .checked_neg()
                .filter(|errno| *errno > 0)
                .unwrap_or(libc::EIO));
        }
        Ok(unsafe { <fd::OwnedFd as fd::FromRawFd>::from_raw_fd(self.0) })
    }
}
