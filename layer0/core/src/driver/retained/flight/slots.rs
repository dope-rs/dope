use std::marker;

use o3::depot::arena::slab;

use crate::{
    driver,
    driver::{
        life::native::{
            request,
            request::{bound, permits},
        },
        result,
        retained::{
            flight,
            flight::{GENERATION_BITS, INDEX_BITS, Invariant, Target, storage},
        },
        route,
        route::kind,
    },
    io::fd::handles,
};

#[doc(hidden)]
#[repr(transparent)]
pub struct Slots<'d, Tag: route::Tag> {
    slots: storage::Slots<'d, Target, INDEX_BITS, GENERATION_BITS>,
    driver: Invariant<'d>,
    tag: marker::PhantomData<Tag>,
}

impl<'d, Tag: route::Tag> Slots<'d, Tag> {
    pub(super) fn new(
        slots: storage::Slots<'d, Target, INDEX_BITS, GENERATION_BITS>,
        driver: Invariant<'d>,
    ) -> Self {
        Self {
            slots,
            driver,
            tag: marker::PhantomData,
        }
    }

    pub(crate) fn try_vacancy<C: kind::Flight>(
        &self,
        target: route::Operation<'d, Tag, C>,
    ) -> Result<flight::Vacant<'_, 'd, Tag, C>, slab::Vacancy> {
        let vacancy = self.slots.try_vacancy()?;
        Ok(flight::Vacant {
            vacancy,
            target: Target::new(target),
            driver: self.driver,
            class: marker::PhantomData,
        })
    }

    pub(in crate::driver::retained) fn try_admit<'a, C: kind::Flight>(
        &self,
        driver: &'a mut driver::Context<'_, 'd>,
        target: route::Operation<'d, Tag, C>,
    ) -> Result<(permits::Admission<'a, 'd>, flight::Vacant<'_, 'd, Tag, C>), result::SubmitError>
    {
        let space = self.try_vacancy(target)?;
        let permit = driver
            .native()
            .try_admit()
            .map_err(result::SubmitError::queue)?;
        Ok((permit, space))
    }

    fn try_submit<C: kind::Flight>(
        &self,
        mut driver: driver::Context<'_, 'd>,
        target: route::Operation<'d, Tag, C>,
        raw: request::Lowered,
    ) -> Result<flight::Flight<'d, C>, result::SubmitError> {
        let (permit, space) = self.try_admit(&mut driver, target)?;
        Ok(permit.submit(bound::Bound::scoped(raw, space)))
    }
}
impl<'d, Tag: route::Tag + route::Select<kind::Recv>> Slots<'d, Tag> {
    pub fn recv(
        &self,
        driver: driver::Context<'_, 'd>,
        fd: &handles::Descriptor<'d>,
        target: route::Target<'d, Tag>,
    ) -> Result<flight::Flight<'d, kind::Recv>, result::SubmitError> {
        let raw = request::Lowered::recv(fd.slot_ref());
        self.try_submit(driver, target.operation::<kind::Recv>(), raw)
    }
}

impl<'d, Tag: route::Tag + route::Select<kind::Send>> Slots<'d, Tag> {
    pub fn shutdown(
        &self,
        driver: driver::Context<'_, 'd>,
        fd: &handles::Descriptor<'d>,
        target: route::Target<'d, Tag>,
    ) -> Result<flight::Flight<'d, kind::Send>, result::SubmitError> {
        let raw = request::Lowered::shutdown(fd.slot_ref());
        self.try_submit(driver, target.operation::<kind::Send>(), raw)
    }
}

impl<'d, Tag: route::Tag> Slots<'d, Tag> {
    pub fn recv_dgram(
        &self,
        mut driver: driver::Context<'_, 'd>,
        fd: &handles::Datagram<'d>,
        target: route::Operation<'d, Tag, kind::Recv>,
    ) -> Result<flight::Flight<'d, kind::Recv>, result::SubmitError> {
        let recv = request::Datagram::new(fd);
        let (permit, space) = self.try_admit(&mut driver, target)?;
        let entry = bound::Bound::scoped(recv, space);
        Ok(permit.submit(entry.into_raw()))
    }
}

impl<'d, Tag: route::Tag> Slots<'d, Tag> {
    pub fn accept(
        &self,
        driver: driver::Context<'_, 'd>,
        fd: &handles::Descriptor<'d>,
        target: route::Operation<'d, Tag, kind::Accept>,
    ) -> Result<flight::Flight<'d, kind::Accept>, result::SubmitError> {
        let raw = request::Lowered::accept_multi(fd.slot_ref());
        self.try_submit(driver, target, raw)
    }
}
