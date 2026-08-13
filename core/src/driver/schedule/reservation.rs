use crate::driver::{
    self, retained,
    schedule::{self, credits},
};

struct Guard<'turn, 'd, 'a, C: ?Sized> {
    context: &'a mut C,
    quota: credits::Quota<'turn, 'd>,
}

#[repr(transparent)]
pub struct Application<'turn, 'a, 'c, 'd>(Guard<'turn, 'd, 'a, driver::Context<'c, 'd>>);

#[repr(transparent)]
pub struct Retained<'turn, 'a, 'c, 'owner, 'd: 'owner>(
    Guard<'turn, 'd, 'a, retained::Context<'c, 'owner, 'd>>,
);

impl<'turn, 'a, 'c, 'd> Application<'turn, 'a, 'c, 'd> {
    pub fn reserve(
        work: schedule::Application<'turn, 'd>,
        context: &'a mut driver::Context<'c, 'd>,
        count: usize,
    ) -> Option<Self> {
        let quota = credits::Quota::from_application(work, count)?;
        Some(Self(Guard { context, quota }))
    }

    pub fn commit(self, count: usize) -> &'a mut driver::Context<'c, 'd> {
        let Guard { context, mut quota } = self.0;
        quota.spend(count);
        drop(quota);
        context
    }
}

impl<'turn, 'a, 'c, 'owner, 'd: 'owner> Retained<'turn, 'a, 'c, 'owner, 'd> {
    pub fn reserve(
        work: schedule::Application<'turn, 'd>,
        context: &'a mut retained::Context<'c, 'owner, 'd>,
        count: usize,
    ) -> Result<Self, &'a mut retained::Context<'c, 'owner, 'd>> {
        let Some(quota) = credits::Quota::from_application(work, count) else {
            return Err(context);
        };
        Ok(Self(Guard { context, quota }))
    }

    pub fn commit(self, count: usize) -> &'a mut retained::Context<'c, 'owner, 'd> {
        let Guard { context, mut quota } = self.0;
        quota.spend(count);
        drop(quota);
        context
    }
}
