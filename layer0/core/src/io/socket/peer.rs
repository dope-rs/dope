use dope_sys::{engine, io::socket::option::tuning};

use crate::{
    Engine, driver,
    driver::{
        life::native::Apply,
        retained::flight,
        route::{self, indices, kind},
    },
    io::{
        fd::handles::Descriptor,
        socket::{option, pending},
    },
};

/// The same descriptor and its observed peer UID, or its pending native owner.
pub enum Query<'d> {
    /// `None` means that the native socket supplied no peer UID.
    Ready(Descriptor<'d>, Option<u32>),
    Pending(pending::Tuning<'d, pending::Live, u32>),
}

impl<'d> Query<'d> {
    pub fn try_new<Tag: route::Tag + route::Select<kind::Tuning>>(
        driver: &mut driver::Context<'_, 'd>,
        slots: &flight::Slots<'d, Tag>,
        target: route::Target<'d, Tag>,
        fd: Descriptor<'d>,
    ) -> Result<Self, pending::Rejection<'d>> {
        use tuning::device::{Owner as _, Source};

        let owner = Apply::<_, u32>::new(driver.native(), slots, target, fd, ());
        let request = <crate::Backend as Source>::Socket::new(owner);
        tuning::raw::Peer::<Engine, _, { option::MAX_STREAM_OPTIONS }>::try_peer(request)
    }

    pub(crate) fn from_native(
        finish: tuning::device::Finish<
            Descriptor<'d>,
            flight::Reserved<'d, kind::Tuning>,
            <Engine as engine::Cancel<indices::SlotIndex>>::Permit,
            u32,
        >,
    ) -> Self {
        match finish {
            tuning::device::Finish::Applied(fd, value) => Self::Ready(fd, uid(value)),
            tuning::device::Finish::Pending(fd, target, permit) => {
                Self::Pending(pending::Tuning::new(fd, target, permit))
            }
        }
    }
}

pub(super) fn uid(value: u32) -> Option<u32> {
    (value != u32::MAX).then_some(value)
}
