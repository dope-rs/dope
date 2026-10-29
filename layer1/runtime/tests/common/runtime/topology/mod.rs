use std::{cell::Cell, convert::Infallible, marker::PhantomData};

use dope_core::driver::{
    route::KeyTag,
    settings,
    settings::{claim, errors},
    storage,
};
use dope_runtime::{
    executor::{self, boot},
    topology::{self, lending, recipe},
};

use crate::common::scenario::app::Empty;

pub mod chains;
pub mod stages;

pub struct Profile;

impl settings::Profile for Profile {
    const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<8, 8>();
    const SCHEDULER: settings::Schedule = settings::Schedule::fixed::<1, 1>();
    const FILES: settings::FileSlots = settings::FileSlots::fixed::<0, 1>();
    const RECEIVE: settings::Receive = settings::Receive::fixed::<8, 64>();
}

pub const PROFILE_CONFIG: settings::Config = settings::Config::for_profile::<Profile>();

pub struct Storage(pub claim::Claim);

impl settings::Resource for Storage {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        needs.include(self.0)
    }
}

impl storage::Plan for Storage {
    type Output<'p, 'd>
        = ()
    where
        Self: 'd,
        'd: 'p;
}

impl storage::Build for Storage {
    type Error = (bool, bool);

    type Store<'d>
        = ()
    where
        Self: 'd;

    fn try_store<'d>(&self) -> Result<Self::Store<'d>, Self::Error>
    where
        Self: 'd,
    {
        Ok(())
    }

    fn try_build<'p, 'd>(
        self,
        _: &'p mut Self::Store<'d>,
        context: storage::Context<'_, 'd>,
    ) -> Result<(), Self::Error>
    where
        Self: 'd,
        'd: 'p,
    {
        let alarm = context.timer().try_alarm();
        match (alarm.is_ok(), context.timer().try_alarm().is_err()) {
            (true, true) => Ok(()),
            observed => Err(observed),
        }
    }
}

pub struct Install(pub claim::Claim);

impl settings::Resource for Install {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        needs.include(self.0)
    }
}

impl topology::Installer<Storage> for Install {
    type App<'life, 'p, 'd>
        = Empty
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        Storage: 'd;
    type Error = Infallible;

    fn install<'scope, 'life, 'p, 'd, Q>(
        self,
        stage: &mut topology::Stage<'_, 'scope, 'life, 'p, 'd, Storage, Q>,
    ) -> Result<Self::App<'life, 'p, 'd>, Self::Error>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        'life: 'scope,
        Self: 'd,
        Storage: 'd,
        Q: executor::Startup,
    {
        let () = *stage.storage();
        Ok(Empty {})
    }
}

pub struct Borrowed<'a>(pub &'a u8);

pub struct Lending<'a> {
    pub value: &'a u8,
    pub reject: bool,
}

#[derive(dope_gen::Application)]
#[dispatcher(core = ::dope_core, manifold = ::dope_manifold, runtime = ::dope_runtime, driver = 'd)]
pub struct Local<'a, 'd> {
    #[dispatcher(state)]
    pub value: &'a u8,
    #[dispatcher(state)]
    driver: PhantomData<&'d ()>,
}

impl settings::Resource for Lending<'_> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        Borrowed(self.value).include_in(needs)
    }
}

impl Lending<'_> {
    pub fn install<'stage, 'scope, 'life, 'p, 'd, S: storage::Plan, Q, R>(
        self,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'p, 'd, S, Q>,
        next: impl for<'local> FnOnce(
            Local<'local, 'd>,
            &mut lending::Stage<'stage, 'scope, 'life, 'p, 'd, S, Q>,
        ) -> R,
    ) -> Result<R, u8> {
        if self.reject {
            return Err(*self.value);
        }
        let value = *self.value;
        Ok(next(
            Local {
                value: &value,
                driver: PhantomData,
            },
            stage,
        ))
    }
}

impl settings::Resource for Borrowed<'_> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        needs.include(claim::Claim::new(settings::Routes::singleton::<KeyTag<2>>()))
    }
}

impl<'a> topology::Installer<storage::Value<&'a u8>> for Borrowed<'a> {
    type App<'life, 'p, 'd>
        = Empty
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        storage::Value<&'a u8>: 'd;
    type Error = (u8, u8);

    fn install<'scope, 'life, 'p, 'd, Q>(
        self,
        stage: &mut topology::Stage<'_, 'scope, 'life, 'p, 'd, storage::Value<&'a u8>, Q>,
    ) -> Result<Self::App<'life, 'p, 'd>, Self::Error>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        'life: 'scope,
        Self: 'd,
        storage::Value<&'a u8>: 'd,
        Q: executor::Startup,
    {
        match (*self.0, **stage.storage()) {
            (7, 11) => Ok(Empty {}),
            observed => Err(observed),
        }
    }
}

pub struct YieldOnce<'a>(pub bool, pub &'a Cell<bool>);

#[derive(Default)]
pub struct Admission {
    pub polls: Cell<usize>,
    pub cancelled: Cell<usize>,
}

impl<'d> boot::Root<'d> for &Admission {
    type Output = Result<Empty, Infallible>;

    fn poll(self, _: boot::Context<'_, 'd>) -> boot::Poll<Self, Self::Output> {
        self.polls.set(self.polls.get() + 1);
        boot::Poll::Ready(Ok(Empty {}))
    }

    fn cancel(self) {
        self.cancelled.set(self.cancelled.get() + 1);
    }
}

impl<'d> boot::Root<'d> for YieldOnce<'_> {
    type Output = Result<Empty, Infallible>;

    fn poll(self, context: boot::Context<'_, 'd>) -> boot::Poll<Self, Self::Output> {
        let (wake, work, clock, cause) = context.into_parts();
        self.1.set(
            self.1.get()
                && work.remaining() > 0
                && work.clock().now() == clock.now()
                && matches!(
                    (self.0, cause),
                    (false, boot::Cause::Start) | (true, boot::Cause::Wake)
                ),
        );
        if self.0 {
            boot::Poll::Ready(Ok(Empty {}))
        } else {
            wake.wake();
            boot::Poll::Pending(Self(true, self.1))
        }
    }

    fn cancel(self) {
        self.1.set(self.1.get() && self.0);
    }
}

type Pair = (storage::Value<String>, storage::Value<u64>);

pub struct Name;

impl recipe::Build<Pair> for Name {
    type Output<'life, 'p, 'd>
        = &'life String
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        Pair: 'd;
    type Error = Infallible;

    fn build<'life, 'p, 'd>(
        self,
        storage: &'life (String, u64),
    ) -> Result<Self::Output<'life, 'p, 'd>, Self::Error>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        Pair: 'd,
    {
        match self {
            Self => Ok(&storage.0),
        }
    }
}

pub struct Number;

impl recipe::Build<Pair> for Number {
    type Output<'life, 'p, 'd>
        = &'life u64
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        Pair: 'd;
    type Error = Infallible;

    fn build<'life, 'p, 'd>(
        self,
        storage: &'life (String, u64),
    ) -> Result<Self::Output<'life, 'p, 'd>, Self::Error>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        Pair: 'd,
    {
        match self {
            Self => Ok(&storage.1),
        }
    }
}
