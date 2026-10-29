use crate::{
    Errno,
    io::socket::option::tuning::device::{Admission, Failure, Finish, Options, Reserve},
};

/// # Safety
/// The backend, descriptor, slot and index share one driver and immutable file extent; a ring's pinned table has that extent.
/// Admit only an idle slot; retain its descriptor and values through settlement or quiescence, and recycle only after every prior chain settles.
pub unsafe trait Host<B, const N: usize>: Sized {
    type File;
    type Slot: Copy;
    type Index: Copy;
    type Reserve: Reserve;
    type Options<'o>: Options<N>;
    type Permit;
    type Output;
    type Error;

    /// Applies synchronous options without issuing a flight reservation or
    /// a pinned-table index. The descriptor and native slot retain `with`'s
    /// exact owner and backend relationship throughout the callback.
    fn direct(
        self,
        run: impl for<'o> FnOnce(
            &mut B,
            Self::File,
            Self::Slot,
            Option<Self::Options<'o>>,
        ) -> Result<Self::File, (Self::File, Errno)>,
    ) -> Result<Self::Output, Self::Error>;

    fn with(
        self,
        run: impl for<'o> FnOnce(
            &mut B,
            Self::File,
            Self::Slot,
            Self::Index,
            Option<Self::Options<'o>>,
            Self::Reserve,
        ) -> Result<
            Finish<Self::File, <Self::Reserve as Reserve>::Reservation, Self::Permit>,
            Failure<Self::File, <Self::Reserve as Reserve>::Error>,
        >,
    ) -> Result<Self::Output, Self::Error>;

    fn apply(
        self,
        run: impl for<'o> FnOnce(
            Admission<'_, B, Self::File, Self::Slot, Self::Index>,
            Option<Self::Options<'o>>,
            Self::Reserve,
        ) -> Result<
            Finish<Self::File, <Self::Reserve as Reserve>::Reservation, Self::Permit>,
            Failure<Self::File, <Self::Reserve as Reserve>::Error>,
        >,
    ) -> Result<Self::Output, Self::Error> {
        self.with(|backend, file, slot, index, options, reserve| {
            run(
                Admission {
                    backend,
                    file,
                    slot,
                    index,
                },
                options,
                reserve,
            )
        })
    }
}

/// # Safety
/// Satisfy `Host`'s owner/extent contract; `slot`, `target` and the terminal must address the supplied descriptor.
/// Retain every borrowed terminal address through completion or quiescence; pinned option storage is required only on a ring backend.
pub unsafe trait Connect<B, R, const N: usize>: Sized {
    type File;
    type Slot: Copy;
    type Index: Copy;
    type Reserve: Reserve;
    type Options<'o>: Options<N>;
    type Output;
    type Error;

    fn slot(file: &Self::File) -> Self::Slot;
    fn target(file: &Self::File) -> (Self::Slot, Self::Index);

    fn with(
        self,
        run: impl for<'o> FnOnce(
            &mut B,
            Self::File,
            Option<Self::Options<'o>>,
            Self::Reserve,
            R,
        ) -> Result<
            (Self::File, <Self::Reserve as Reserve>::Reservation),
            Failure<Self::File, <Self::Reserve as Reserve>::Error>,
        >,
    ) -> Result<Self::Output, Self::Error>;

    /// Synchronous options need a native slot only when an option is present.
    fn direct(
        self,
        run: impl for<'o> FnOnce(
            &mut B,
            Self::File,
            Option<(Self::Slot, Self::Options<'o>)>,
            Self::Reserve,
            R,
        ) -> Result<
            (Self::File, <Self::Reserve as Reserve>::Reservation),
            Failure<Self::File, <Self::Reserve as Reserve>::Error>,
        >,
    ) -> Result<Self::Output, Self::Error> {
        self.with(|backend, file, options, reserve, last| {
            let options = options.map(|options| (Self::slot(&file), options));
            run(backend, file, options, reserve, last)
        })
    }

    /// Admit a pinned option chain only after selecting a nonempty option set.
    fn connect(
        self,
        plain: impl FnOnce(
            &mut B,
            Self::File,
            Self::Reserve,
            R,
        ) -> Result<
            (Self::File, <Self::Reserve as Reserve>::Reservation),
            Failure<Self::File, <Self::Reserve as Reserve>::Error>,
        >,
        tuned: impl for<'o> FnOnce(
            Admission<'_, B, Self::File, Self::Slot, Self::Index>,
            Self::Options<'o>,
            Self::Reserve,
            R,
        ) -> Result<
            (Self::File, <Self::Reserve as Reserve>::Reservation),
            Failure<Self::File, <Self::Reserve as Reserve>::Error>,
        >,
    ) -> Result<Self::Output, Self::Error> {
        self.with(|backend, file, options, reserve, last| {
            let Some(options) = options else {
                return plain(backend, file, reserve, last);
            };
            let (slot, index) = Self::target(&file);
            tuned(
                Admission {
                    backend,
                    file,
                    slot,
                    index,
                },
                options,
                reserve,
                last,
            )
        })
    }
}
