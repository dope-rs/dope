use std::{
    cell::Cell,
    convert::Infallible,
    pin::Pin,
    ptr::{self, NonNull},
};

use dope_core::driver::{self, schedule, settings};
use dope_runtime::{
    client::{Bound, Context, Provider},
    executor::{self, session},
    topology::lending,
};

pub struct Owner<'a> {
    pub events: &'a Cell<u64>,
    pub address: &'a Cell<*const ()>,
}

impl Owner<'_> {
    fn at_residence(&self) {
        assert_eq!(self.address.get(), ptr::from_ref(self).cast());
    }
}

impl Drop for Owner<'_> {
    fn drop(&mut self) {
        if !self.address.get().is_null() {
            self.at_residence();
        }
        self.events.set((self.events.get() << 4) | 9);
    }
}

pub struct Backing<'a>(pub &'a Cell<u64>);

impl Drop for Backing<'_> {
    fn drop(&mut self) {
        assert_eq!(self.0.get() & 0xf, 9);
        self.0.set((self.0.get() << 4) | 0xa);
    }
}

// SAFETY: this fixture retains no native operation; its late cleanup is one cell transition.
unsafe impl<'d> lending::raw::Resume<'d> for Owner<'_> {}

pub struct Client<'app, 'd: 'app, 'a>(Bound<'app, 'd, Owner<'a>, NonNull<Cell<u64>>>);

impl Drop for Client<'_, '_, '_> {
    fn drop(&mut self) {
        // SAFETY: the exact provider retains this cell through client destruction.
        let state = unsafe { self.0.as_ref() };
        assert!(matches!(state.get(), 1 | 0x1234));
        state.set((state.get() << 4) | 5);
    }
}

impl<'d, 'a> Provider<'d> for Owner<'a> {
    type Client<'app>
        = Client<'app, 'd, 'a>
    where
        Self: 'app,
        'd: 'app;

    fn provide<'app>(context: Context<'_, 'app, 'd, Self>) -> Self::Client<'app>
    where
        Self: 'app,
        'd: 'app,
    {
        let (owner, bound) = context.into_parts();
        owner.address.set(ptr::from_ref(owner.get_ref()).cast());
        owner.events.set(1);
        Client(bound.bind(NonNull::from(owner.events)))
    }
}

impl<'d> executor::raw::Layout<'d> for Owner<'_> {
    type ParkPlan = ();
}

unsafe impl<'d> executor::raw::App<'d> for Owner<'_> {
    const ROUTES: settings::Routes = settings::Routes::EMPTY;
    const COORDS: usize = 1;

    fn install<'app, 'owner>(
        call: executor::raw::Install<'app, 'owner, 'd, Self>,
    ) -> executor::raw::Install<'app, 'owner, 'd, Self> {
        let (app, install) = call.into_parts();
        app.at_residence();
        assert_eq!(app.events.get(), 1);
        app.events.set(0x12);
        executor::raw::Install::new(app, install)
    }

    fn released(mut call: executor::raw::Released<'_, Self>) -> executor::raw::Released<'_, Self> {
        call.owner().at_residence();
        assert_eq!(call.owner().events.get(), 0x12345);
        call.owner().events.set(0x123456);
        call
    }

    fn progress(_: Pin<&Self>, _: executor::raw::Progress<'_, '_, 'd>) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }

    fn stopping(app: Pin<&Self>, _: executor::raw::Progress<'_, '_, 'd>) -> schedule::Progress<'d> {
        if app.events.get() == 0x123456 {
            schedule::Progress::Runnable
        } else {
            schedule::Progress::Quiescent
        }
    }
    fn select_park() -> Option<Self::ParkPlan> {
        Some(())
    }

    unsafe fn park<'app, 'driver, 'turn, 'root>(
        app: Pin<&'app mut Self>,
        _: &mut Self::ParkPlan,
        mut park: executor::raw::Park<'driver, 'turn, 'root, 'd>,
    ) where
        'd: 'driver + 'turn + 'root,
    {
        if app.events.get() == 0x123456 && park.next_step().is_some() {
            app.events.set(0x1234567);
        }
    }

    fn shutdown<'app, 'turn, 'driver, 'owner>(
        call: executor::raw::Shutdown<'app, 'turn, 'driver, 'owner, 'd, Self>,
    ) -> executor::raw::Shutdown<'app, 'turn, 'driver, 'owner, 'd, Self>
    where
        'd: 'owner,
    {
        let (app, turn, driver) = call.into_parts();
        assert_eq!(app.events.get(), 0x123);
        app.events.set(0x1234);
        executor::raw::Shutdown::new(app, turn, driver)
    }

    fn finish<'app, 'finish, 'owner, 'epoch>(
        call: executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self>,
    ) -> executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self> {
        let (app, install, finish) = call.into_parts();
        app.at_residence();
        assert_eq!(app.events.get(), 0x1234567);
        app.events.set(0x12345678);
        executor::raw::Finish::new(app, install, finish)
    }
}

pub struct Create<'a>(pub &'a Cell<u64>);

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(core = ::dope_core, manifold = ::dope_manifold, runtime = ::dope_runtime, driver = 'd)]
pub struct App<'app, 'd: 'app, 'a> {
    #[dispatcher(state)]
    client: Client<'app, 'd, 'a>,
}

impl<'a, 'd> lending::Build<'d, Owner<'a>> for Create<'a> {
    type App<'app>
        = App<'app, 'd, 'a>
    where
        Self: 'app,
        Owner<'a>: 'app,
        'd: 'app;
    type Data = &'a Cell<u64>;
    type Error = Infallible;

    fn build<'app>(
        self,
        client: Client<'app, 'd, 'a>,
        _: driver::Context<'_, 'd>,
    ) -> Result<(Self::App<'app>, Self::Data), Self::Error>
    where
        Self: 'app,
        Owner<'a>: 'app,
        'd: 'app,
    {
        Ok((App { client }, self.0))
    }
}

impl<'a, 'life, 'd: 'life, Q> lending::Execute<'life, 'd, Owner<'a>, Q> for Create<'a> {
    type Output = ();

    fn run<'app, 'run>(
        state: Self::Data,
        _: session::App<
            'run,
            'life,
            'd,
            lending::raw::Bound<'app, 'd, Owner<'a>, Self::App<'app>>,
            Q,
        >,
    ) where
        Self: 'app,
        Owner<'a>: 'app,
        'd: 'app,
        'app: 'run,
        'life: 'run,
    {
        assert_eq!(state.get(), 0x12);
        state.set(0x123);
    }
}

pub struct Borrowed<'state, 'app, 'd: 'app, 'a>(pub Pin<&'state Client<'app, 'd, 'a>>);

impl<'d> executor::raw::Layout<'d> for Borrowed<'_, '_, 'd, '_> {
    type ParkPlan = Infallible;
}

// SAFETY: this application retains only its exact callback-local client borrow.
unsafe impl<'d> executor::raw::App<'d> for Borrowed<'_, '_, 'd, '_> {
    const ROUTES: settings::Routes = settings::Routes::EMPTY;
    const COORDS: usize = 0;

    fn select_park() -> Option<Self::ParkPlan> {
        None
    }

    unsafe fn park<'app, 'driver, 'turn, 'root>(
        _: Pin<&'app mut Self>,
        plan: &mut Self::ParkPlan,
        _: executor::raw::Park<'driver, 'turn, 'root, 'd>,
    ) where
        'd: 'driver + 'turn + 'root,
    {
        match *plan {}
    }

    fn install<'app, 'owner>(
        call: executor::raw::Install<'app, 'owner, 'd, Self>,
    ) -> executor::raw::Install<'app, 'owner, 'd, Self> {
        let (app, install) = call.into_parts();
        // SAFETY: this retained client keeps the exact provider cell alive.
        let state = unsafe { app.0.get_ref().0.as_ref() };
        assert_eq!(state.get(), 0x12);
        state.set(0x123);
        executor::raw::Install::new(app, install)
    }

    fn progress(_: Pin<&Self>, _: executor::raw::Progress<'_, '_, 'd>) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }

    fn stopping(_: Pin<&Self>, _: executor::raw::Progress<'_, '_, 'd>) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }
}
