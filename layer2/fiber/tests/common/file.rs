use std::{
    cell::Cell,
    env, fs, mem,
    ops::ControlFlow,
    os::fd::{AsRawFd as _, RawFd},
    pin::Pin,
};

use dope::{
    core::{
        Backend, Errno,
        driver::{schedule, settings},
        io,
    },
    manifold::{
        app::{dispatch, dispatch::event},
        file::read,
    },
    runtime::executor,
};
use o3::buffer::{resident, storage::Owned};

pub fn fixture(
    name: &str,
) -> dope_test::harness::support::file::Directory<
    impl FnOnce(Result<(), dope_test::harness::support::file::Error>),
> {
    let directory =
        dope_test::harness::support::file::Directory::try_with(&env::temp_dir(), name, |result| {
            result.expect("remove fixture")
        })
        .expect("fixture directory");
    fs::write(directory.path().join("data"), b"abc").expect("fixture contents");
    directory
}

pub fn open(directory: &io::fs::Dir, name: &str) -> io::fs::OpenRequest {
    let name = io::fs::path::Component::parse_os(name.as_ref()).expect("one component");
    io::fs::OpenRequest::new(
        directory.confine_name(name).expect("confined name"),
        io::fs::open::Options::new(io::fs::open::Access::Read),
    )
}

pub fn buffer<'d>(budget: &resident::Budget<'d>) -> read::Buffer<'d> {
    Owned::try_allocate(budget, 8).expect("charged output")
}

pub struct Profile;

fn identity(fd: RawFd) -> Result<(u128, u128), Errno> {
    let mut stat = mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: fstat accepts a numeric descriptor, including a closed or reused
    // one, and writes only this complete native stat storage on success.
    if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } != 0 {
        // SAFETY: the failed native call left this thread's errno unchanged.
        return Err(unsafe { dope::core::dope_sys::kernel::raw::Code::last::<Backend>() }.into());
    }
    // SAFETY: successful fstat initialized the complete native value.
    let stat = unsafe { stat.assume_init() };
    Ok((stat.st_dev as u128, stat.st_ino as u128))
}

impl settings::Profile for Profile {
    const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<64, 128>();
    const SCHEDULER: settings::Schedule = settings::Schedule::fixed::<16, 1>();
    const RECEIVE: settings::Receive = settings::Receive::fixed::<2, 4096>();
    const RESIDENT_DOMAINS: settings::Domain = settings::Domain::ONE;
}

/// Observes the actual installed owner after submission, before the next CQ drain.
#[pin_project::pin_project]
pub struct Observed<'a, M> {
    #[pin]
    pub reader: M,
    pub submitted: &'a Cell<bool>,
    pub on_completion: Option<&'a dyn Fn() -> bool>,
}

impl<'d, M: dispatch::raw::Manifold<'d>> dispatch::raw::Projection<'d> for Observed<'_, M> {
    type Inner = M;

    fn inner(self: Pin<&Self>) -> Pin<&M> {
        self.project_ref().reader
    }
    fn inner_mut(self: Pin<&mut Self>) -> Pin<&mut M> {
        self.project().reader
    }
}

// SAFETY: every operation preserves the same installed owner and its loans.
// The observation only reads its existing lifecycle capability.
unsafe impl<'d, M> dispatch::raw::Manifold<'d> for Observed<'_, M>
where
    M: dispatch::raw::Manifold<'d, PrePark = dispatch::raw::Retained>
        + event::Drive<'d, Access = dispatch::raw::Retained>,
{
    type Route = M::Route;
    type Dispatch = event::Driven<dispatch::raw::Retained>;
    type Install = dispatch::raw::Projected<Self>;
    type Finish = dispatch::raw::Projected<Self>;
    type Activate = dispatch::raw::Projected<Self>;
    type Probe = dispatch::raw::Projected<Self>;
    type PrePark = dispatch::raw::Retained;
    type Shutdown = M::Shutdown;
    const COORDS: usize = M::COORDS;

    unsafe fn pre_park<'a, 'turn>(
        mut self: Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'd, Self::PrePark>,
    ) -> Pin<&'a mut Self> {
        // SAFETY: projection retains the exact installed owner and its loans.
        unsafe { M::pre_park(self.as_mut().project().reader, turn, driver) };
        let this = self.as_ref().project_ref();
        if matches!(
            <M::Probe as dispatch::raw::Probe<'d, M>>::progress(
                this.reader,
                driver.regions(),
                driver.driver_ref().clock()
            ),
            schedule::Progress::Waiting(_)
        ) {
            this.submitted.set(true);
        }
        self
    }

    fn shutdown<'a, 'turn>(
        mut self: Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'd, Self::Shutdown>,
    ) -> Pin<&'a mut Self> {
        M::shutdown(self.as_mut().project().reader, turn, driver);
        self
    }
}

impl<'d, M> event::Drive<'d> for Observed<'_, M>
where
    M: dispatch::raw::Manifold<'d> + event::Drive<'d, Access = dispatch::raw::Retained>,
{
    type Access = dispatch::raw::Retained;

    fn dispatch<'app, 'driver>(
        call: executor::raw::Call<
            'app,
            Self,
            (),
            (
                io::Event<'d>,
                dispatch::raw::Context<'driver, 'd, Self::Access>,
            ),
        >,
    ) -> ControlFlow<io::Event<'d>> {
        let (owner, (), (event, driver)) = call.into_parts();
        let this = owner.project();
        let event = event.into_kind();
        let completed = match &event {
            io::event::Kind::Read(done) => done.outcome().is_ok(),
            io::event::Kind::Open(done) => {
                matches!(done.outcome(), io::event::file::open::Outcome::Opened(_))
            }
            _ => false,
        };
        let detached = completed && this.on_completion.is_some_and(|complete| complete());
        let retired = if detached
            && let io::event::Kind::Open(done) = &event
            && let io::event::file::open::Outcome::Opened(fd) = done.outcome()
        {
            let fd = fd.as_raw_fd();
            let before = identity(fd).expect("successful Open owns a live descriptor");
            Some((fd, before))
        } else {
            None
        };
        let result = <M as event::Drive<'d>>::dispatch(executor::raw::Call::new(
            this.reader,
            (),
            (event.into(), driver),
        ));
        if let Some((fd, before)) = retired {
            assert!(
                result.is_continue(),
                "Reader consumes the original successful Open"
            );
            match identity(fd) {
                Err(cause) => assert_eq!(cause, Errno::BAD_DESCRIPTOR),
                Ok(after) => assert_ne!(
                    after, before,
                    "the original unique file owner must be released, even if its descriptor number was reused"
                ),
            }
        }
        result
    }
}
