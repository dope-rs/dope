//! Runtime client integration coverage.

use std::{cell::RefCell, pin::Pin, task::Poll, time::Duration};

use dope_core::driver::{
    schedule::{self, clock, clock::timer},
    settings,
};
use dope_runtime::{
    client::{Bound, Context, Provider},
    executor::{self, root},
    topology::lending,
};

#[derive(dope_gen::Application)]
#[dispatcher(core = ::dope_core, manifold = ::dope_manifold, runtime = ::dope_runtime)]
pub struct App {
    #[dispatcher(state)]
    pub value: u8,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Lifecycle {
    Installed,
    Stopped,
    Finished,
    Dropped,
}

pub struct Rejected<'a, const PARK: bool>(pub &'a RefCell<Vec<Lifecycle>>);

impl<'d, const PARK: bool> executor::raw::Layout<'d> for Rejected<'_, PARK> {
    type ParkPlan = ();
}

// SAFETY: this application has no pinned fields or routes, and each lifecycle
// operation retains the exact application and authority it receives.
unsafe impl<'d, const PARK: bool> executor::raw::App<'d> for Rejected<'_, PARK> {
    const ROUTES: settings::Routes = settings::Routes::EMPTY;
    const COORDS: usize = schedule::LIMIT + 1;

    fn admit(
        _: Pin<&Self>,
        needs: &mut settings::claim::Needs,
    ) -> Result<(), settings::errors::Claim> {
        if !PARK {
            let claim = settings::claim::Claim::new(settings::Routes::singleton::<
                dope_core::driver::route::KeyTag<3>,
            >());
            *needs = needs.include(claim)?.include(claim)?;
        }
        Ok(())
    }

    fn install<'app, 'owner>(
        install: executor::raw::Install<'app, 'owner, 'd, Self>,
    ) -> executor::raw::Install<'app, 'owner, 'd, Self> {
        let (app, install) = install.into_parts();
        app.0.borrow_mut().push(Lifecycle::Installed);
        executor::raw::Install::new(app, install)
    }

    fn progress(_: Pin<&Self>, _: executor::raw::Progress<'_, '_, 'd>) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }

    fn stopping(_: Pin<&Self>, _: executor::raw::Progress<'_, '_, 'd>) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }
    fn select_park() -> Option<Self::ParkPlan> {
        PARK.then_some(())
    }

    /// # Safety
    /// The owner and plan must pass installation and coordinator admission.
    /// This fixture exceeds the coordinator limit and must never be invoked.
    unsafe fn park<'app, 'driver, 'turn, 'root>(
        _: Pin<&'app mut Self>,
        _: &mut Self::ParkPlan,
        _: executor::raw::Park<'driver, 'turn, 'root, 'd>,
    ) where
        'd: 'driver + 'turn + 'root,
    {
        unreachable!("rejected")
    }

    fn shutdown<'app, 'turn, 'driver, 'owner>(
        shutdown: executor::raw::Shutdown<'app, 'turn, 'driver, 'owner, 'd, Self>,
    ) -> executor::raw::Shutdown<'app, 'turn, 'driver, 'owner, 'd, Self>
    where
        'd: 'owner,
    {
        let (app, turn, driver) = shutdown.into_parts();
        app.0.borrow_mut().push(Lifecycle::Stopped);
        executor::raw::Shutdown::new(app, turn, driver)
    }

    fn finish<'app, 'finish, 'owner, 'epoch>(
        finish: executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self>,
    ) -> executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self> {
        let (app, install, finish) = finish.into_parts();
        app.0.borrow_mut().push(Lifecycle::Finished);
        executor::raw::Finish::new(app, install, finish)
    }
}

impl<const PARK: bool> Drop for Rejected<'_, PARK> {
    fn drop(&mut self) {
        self.0.borrow_mut().push(Lifecycle::Dropped);
    }
}

pub struct Client<'app, 'd: 'app> {
    pub lease: Bound<'app, 'd, App>,
    pub value: u8,
}

impl<'app, 'd: 'app> Client<'app, 'd> {
    pub fn parts(&self) -> (&Bound<'app, 'd, App>, u8) {
        (&self.lease, self.value)
    }
}

impl<'d> Provider<'d> for App {
    type Client<'app>
        = Client<'app, 'd>
    where
        Self: 'app,
        'd: 'app;

    fn provide<'app>(context: Context<'_, 'app, 'd, Self>) -> Self::Client<'app>
    where
        Self: 'app,
        'd: 'app,
    {
        let (provider, scope) = context.into_parts();
        Client {
            lease: scope,
            value: provider.get_ref().value,
        }
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(core = ::dope_core, manifold = ::dope_manifold, runtime = ::dope_runtime)]
pub struct Owner {
    #[pin]
    #[app]
    pub source: App,
}

impl<'d> lending::Root<'d> for Owner {
    type Provider = App;

    fn provider(self: Pin<&Self>) -> Pin<&App> {
        self.project_ref().source
    }
}

pub struct Immediate<'a>(pub usize, pub &'a mut Option<usize>);

pub struct Pending;

pub struct Alarm<'d> {
    pub alarm: timer::Alarm<'d>,
    pub due: Option<clock::Deadline<'d>>,
}

impl<'d> root::Root<'d> for Alarm<'d> {
    type Output = Result<(clock::Deadline<'d>, clock::Moment<'d>), clock::Range>;

    fn poll(context: root::Context<'_, 'd, Self>) -> Poll<Self::Output> {
        let (root, wake, _, driver) = context.into_parts();
        let this = root.get_mut();
        let now = driver.driver_ref().clock().now();
        let Some(due) = this.due else {
            let due = match now.try_add(Duration::from_millis(1)) {
                Ok(due) => due,
                Err(error) => return Poll::Ready(Err(error)),
            };
            this.due = Some(due);
            this.alarm.arm(due, wake.into());
            return Poll::Pending;
        };
        this.alarm.poll(now, wake.into()).map(|()| Ok((due, now)))
    }
}

impl<'d> root::Root<'d> for Pending {
    type Output = ();

    fn poll(_: root::Context<'_, 'd, Self>) -> Poll<()> {
        Poll::Pending
    }
}

pub const ITERATIONS: usize = 4_096;

impl<'d> root::Root<'d> for Immediate<'_> {
    type Output = usize;

    fn poll(context: root::Context<'_, 'd, Self>) -> Poll<Self::Output> {
        let (root, _, _, _) = context.into_parts();
        let this = root.get_mut();
        *this.1 = this.1.and_then(|polls| polls.checked_add(1));
        Poll::Ready(this.0)
    }
}
