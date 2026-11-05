use std::{cell::Cell, mem::MaybeUninit, ops::ControlFlow, path::Path, pin::Pin};

use dope_core::{
    Backend, Errno,
    dope_sys::kernel::raw::{Code, Status},
    driver::{route::KeyTag, schedule},
    io::{self, event::Kind},
};
use dope_manifold::{
    app::dispatch::{self, event},
    file::appender::{self, Failure, RotateError, WriteError},
};
use dope_runtime::executor;
use dope_test::harness::{scenario::process::Resource, signals::LocalGate};

type Writer<'p, 'd, const N: usize, const B: usize> = appender::Appender<'p, 'd, KeyTag<37>, N, B>;

pub enum Request<'a> {
    Fill(&'a Path),
    Paused,
    Retry,
    Rotate(&'a Path, Option<u8>),
    Append(&'a [u8]),
    Drain,
}

#[derive(Default)]
pub struct Native<'a> {
    pub(crate) request: Cell<Option<Request<'a>>>,
    pub(crate) rotated: Cell<Option<Result<(), RotateError>>>,
    pub(crate) done: LocalGate,
}

/// Observes the real appender immediately after submission, before a completion
/// can retire its first block and unblock the queued second block.
#[pin_project::pin_project]
pub struct Observed<'a, 'p, 'd, const N: usize, const B: usize> {
    #[pin]
    pub(crate) writer: Writer<'p, 'd, N, B>,
    pub(crate) checked: &'a Cell<Option<bool>>,
    pub(crate) native: Option<&'a Native<'a>>,
}

impl<'p, 'd, const N: usize, const B: usize> dispatch::raw::Projection<'d>
    for Observed<'_, 'p, 'd, N, B>
{
    type Inner = Writer<'p, 'd, N, B>;

    fn inner(self: Pin<&Self>) -> Pin<&Self::Inner> {
        self.project_ref().writer
    }

    fn inner_mut(self: Pin<&mut Self>) -> Pin<&mut Self::Inner> {
        self.project().writer
    }
}

// SAFETY: every operation projects the same installed, pinned writer.
// Native completions are observed and forwarded unchanged. Fixture requests use
// the exact installed writer's control without retaining it beyond this call.
unsafe impl<'p, 'd, const N: usize, const B: usize> dispatch::raw::Manifold<'d>
    for Observed<'_, 'p, 'd, N, B>
{
    type Route = <Writer<'p, 'd, N, B> as dispatch::raw::Manifold<'d>>::Route;
    type Dispatch = event::Driven<dispatch::raw::Retained>;
    type Install = dispatch::raw::Projected<Self>;
    type Finish = dispatch::raw::Projected<Self>;
    type Activate = dispatch::raw::Projected<Self>;
    type Probe = dispatch::raw::Projected<Self>;
    type PrePark = dispatch::raw::Retained;
    type Shutdown = <Writer<'p, 'd, N, B> as dispatch::raw::Manifold<'d>>::Shutdown;

    const COORDS: usize = <Writer<'p, 'd, N, B> as dispatch::raw::Manifold<'d>>::COORDS;

    unsafe fn pre_park<'a, 'turn, 'root>(
        mut self: Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'root, 'd, Self::PrePark>,
    ) -> Pin<&'a mut Self>
    where
        Self: 'root,
    {
        // SAFETY: projection retains the exact installed writer and its loans.
        unsafe {
            <Writer<'p, 'd, N, B> as dispatch::raw::Manifold<'d>>::pre_park(
                self.as_mut().project().writer,
                turn,
                driver,
            )
        };
        let this = self.as_ref().project_ref();
        if this.checked.get().is_none() {
            let waiting = matches!(
                <Writer<'p, 'd, N, B> as dispatch::raw::Probe<'d, _>>::progress(
                    this.writer,
                    driver.regions(),
                    driver.driver_ref().clock(),
                ),
                schedule::Progress::Waiting(_)
            );
            this.checked.set(Some(waiting));
        }
        if let Some(native) = *this.native {
            if let Some(request) = native.request.take() {
                let waiting = this.checked.get();
                let drain = {
                    // SAFETY: lifecycle projection installed this exact writer;
                    // the control remains inside its exclusive pre-park loan.
                    let mut control = unsafe {
                        <Writer<'p, 'd, N, B> as dispatch::raw::Control<'d>>::root(
                            self.as_mut().project().writer,
                        )
                    };
                    match request {
                        Request::Fill(path) => {
                            assert_eq!(waiting, Some(true));
                            assert!(matches!(
                                control.try_rotate(path, None),
                                Err(RotateError::Busy)
                            ));
                            assert!(!path.exists());
                            for bytes in [b"IJKLMNOP".as_slice(), b"qrst"] {
                                let mut record =
                                    control.record(bytes.len()).expect("retained block");
                                record.try_extend(bytes).expect("retained bytes");
                                record.commit();
                            }
                            false
                        }
                        Request::Paused => {
                            assert!(
                                matches!(control.record(0), Err(WriteError::Failed(Failure::System(errno))) if errno.code().get() == libc::EFBIG)
                            );
                            native.done.handle().hit();
                            false
                        }
                        Request::Retry => {
                            control.try_retry().expect("explicit retry");
                            true
                        }
                        Request::Rotate(path, tail) => {
                            native.rotated.set(Some(control.try_rotate(path, tail)));
                            native.done.handle().hit();
                            false
                        }
                        Request::Append(bytes) => {
                            let mut record =
                                control.record(bytes.len()).expect("generation record");
                            record.try_extend(bytes).expect("generation bytes");
                            record.commit();
                            true
                        }
                        Request::Drain => true,
                    }
                };
                if drain {
                    let this = self.as_ref().project_ref();
                    if matches!(
                        <Writer<'p, 'd, N, B> as dispatch::raw::Probe<'d, _>>::progress(
                            this.writer,
                            driver.regions(),
                            driver.driver_ref().clock(),
                        ),
                        schedule::Progress::Quiescent
                    ) {
                        native.done.handle().hit();
                    } else {
                        native.request.set(Some(Request::Drain));
                    }
                }
            }
        }
        self
    }

    fn shutdown<'a, 'turn, 'root>(
        mut self: Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'root, 'd, Self::Shutdown>,
    ) -> Pin<&'a mut Self>
    where
        Self: 'root,
    {
        <Writer<'p, 'd, N, B> as dispatch::raw::Manifold<'d>>::shutdown(
            self.as_mut().project().writer,
            turn,
            driver,
        );
        self
    }
}

impl<'p, 'd, const N: usize, const B: usize> event::Drive<'d> for Observed<'_, 'p, 'd, N, B> {
    type Access = dispatch::raw::Retained;

    fn dispatch<'app, 'driver, 'root>(
        call: executor::raw::Call<
            'app,
            Self,
            (),
            (
                io::Event<'d>,
                dispatch::raw::Context<'driver, 'root, 'd, Self::Access>,
            ),
        >,
    ) -> ControlFlow<io::Event<'d>>
    where
        'd: 'root,
        Self: 'root,
    {
        let (owner, (), (event, driver)) = call.into_parts();
        let this = owner.project();
        let event = event.into_kind();
        let failed = matches!(&event, Kind::Write(done) if done.outcome().is_err());
        let result = <Writer<'p, 'd, N, B> as event::Drive<'d>>::dispatch(
            executor::raw::Call::new(this.writer, (), (event.into(), driver)),
        );
        if failed && let Some(native) = *this.native {
            assert!(result.is_continue(), "exact native write completion");
            native.done.handle().hit();
        }
        result
    }
}

pub struct FileSize;

#[derive(Clone, Copy)]
pub struct Handler(libc::sighandler_t);

impl Resource for FileSize {
    type Limits = libc::rlimit;
    type Handler = Handler;

    fn limits() -> Result<Self::Limits, Errno> {
        let mut value = MaybeUninit::uninit();
        // SAFETY: success initializes the complete object; failure retains errno.
        unsafe {
            Status::<-1, 0>::new(libc::getrlimit(libc::RLIMIT_FSIZE, value.as_mut_ptr()))
                .try_zero::<Errno>(|| Code::last::<Backend>().into())?;
            Ok(value.assume_init())
        }
    }

    fn soft(limits: &Self::Limits, value: u64) -> Result<(), Errno> {
        Self::restore(&libc::rlimit {
            rlim_cur: value,
            rlim_max: limits.rlim_max,
        })
    }

    fn restore(limits: &Self::Limits) -> Result<(), Errno> {
        // SAFETY: the native object remains borrowed; errno is read on failure only.
        unsafe {
            Status::<-1, 0>::new(libc::setrlimit(libc::RLIMIT_FSIZE, limits))
                .try_zero(|| Code::last::<Backend>().into())
        }
    }

    fn ignore() -> Result<Self::Handler, Errno> {
        // SAFETY: SIG_IGN is a valid native disposition in this isolated child.
        let handler = unsafe { libc::signal(libc::SIGXFSZ, libc::SIG_IGN) };
        if handler == libc::SIG_ERR {
            // SAFETY: signal failed immediately before this errno read.
            Err(unsafe { Code::last::<Backend>() }.into())
        } else {
            Ok(Handler(handler))
        }
    }

    fn signal(handler: Self::Handler) -> Result<(), Errno> {
        // SAFETY: the private Handler retains a successful native disposition.
        if unsafe { libc::signal(libc::SIGXFSZ, handler.0) } == libc::SIG_ERR {
            // SAFETY: signal failed immediately before this errno read.
            Err(unsafe { Code::last::<Backend>() }.into())
        } else {
            Ok(())
        }
    }
}
