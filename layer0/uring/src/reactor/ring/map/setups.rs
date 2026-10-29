use std::{marker, mem, num, sync::atomic};

use dope_sys::ring::layout::{Layout, Region};
use o3::base::num::bounded;

use crate::reactor::{
    cq,
    ring::{self, Cleanup, Error, map, map::Cause},
    sq,
};

type Parameters = ring::ffi::Parameters;

pub(crate) struct Setup {
    pub(crate) fd: ring::ffi::Fd,
    pub(crate) mapped: Mapped,
    pub(crate) parameters: Parameters,
}

#[derive(Debug)]
pub(crate) struct Mapped {
    sq: sq::raw::State,
    completion: cq::raw::State,
    maps: map::Set,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Axis {
    entries: num::NonZero<u32>,
    mask: u32,
}

pub(crate) struct Offset<T> {
    value: u32,
    item: marker::PhantomData<fn() -> T>,
}

impl<T> Clone for Offset<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Offset<T> {}

pub(crate) struct Submit {
    axis: Axis,
    head: Offset<atomic::AtomicU32>,
    tail: Offset<atomic::AtomicU32>,
    flags: Offset<atomic::AtomicU32>,
    entries: Offset<sq::Entry>,
}

pub(crate) struct Complete {
    axis: Axis,
    head: Offset<atomic::AtomicU32>,
    tail: Offset<atomic::AtomicU32>,
    entries: Offset<cq::Entry<()>>,
}

struct Geometry {
    sq: Submit,
    cq: Complete,
}

struct Plan {
    sq: Axis,
    cq: Axis,
    entries: num::NonZero<usize>,
    rings: num::NonZero<usize>,
}

impl Plan {
    /// Reserves liburing 2.6's 320-byte shared header allowance without the SQ index array.
    /// Geometry admits returned offsets against these owned bytes.
    fn new(sq: num::NonZero<u32>, cq: num::NonZero<u32>) -> Result<Self, Layout> {
        let sq = Self::round(Region::Submission, sq)?;
        let cq = Self::round(Region::Completion, cq)?;
        let entries = Geometry::extent(
            Region::Entries,
            0,
            sq.entries,
            Geometry::stride::<sq::Entry>(),
        )?;
        let rings = Geometry::extent(
            Region::Completion,
            320,
            cq.entries,
            Geometry::stride::<cq::Entry<()>>(),
        )?;
        Ok(Self {
            sq,
            cq,
            entries,
            rings,
        })
    }

    fn round(region: Region, count: num::NonZero<u32>) -> Result<Axis, Layout> {
        let entries = count.checked_next_power_of_two().ok_or(Layout::Entries {
            region,
            actual: Geometry::count(count.get()),
        })?;
        Ok(Axis {
            entries,
            mask: entries.get() - 1,
        })
    }
}

impl Axis {
    pub(crate) const fn entries(self) -> num::NonZero<u32> {
        self.entries
    }

    pub(crate) const fn mask(self) -> u32 {
        self.mask
    }
}

impl<T> Offset<T> {
    pub(crate) const fn get(self) -> u32 {
        self.value
    }
}

impl Submit {
    pub(crate) const fn axis(&self) -> Axis {
        self.axis
    }

    pub(crate) const fn head(&self) -> Offset<atomic::AtomicU32> {
        self.head
    }

    pub(crate) const fn tail(&self) -> Offset<atomic::AtomicU32> {
        self.tail
    }

    pub(crate) const fn flags(&self) -> Offset<atomic::AtomicU32> {
        self.flags
    }

    pub(crate) const fn entries(&self) -> Offset<sq::Entry> {
        self.entries
    }
}

impl Complete {
    pub(crate) const fn axis(&self) -> Axis {
        self.axis
    }

    pub(crate) const fn head(&self) -> Offset<atomic::AtomicU32> {
        self.head
    }

    pub(crate) const fn tail(&self) -> Offset<atomic::AtomicU32> {
        self.tail
    }

    pub(crate) const fn entries(&self) -> Offset<cq::Entry<()>> {
        self.entries
    }
}

impl Geometry {
    fn count(value: u32) -> bounded::U32<0, { u32::MAX }> {
        bounded::U32::clamp(value as usize)
    }

    fn size(value: usize) -> bounded::U64<0, { u64::MAX }> {
        const { assert!(usize::BITS <= u64::BITS) };
        bounded::U64::clamp(value as u64)
    }

    fn new(params: &Parameters, plan: &Plan) -> Result<Self, Layout> {
        let sq = Self::axis(Region::Submission, params.sq_entries, plan.sq)?;
        let cq = Self::axis(Region::Completion, params.cq_entries, plan.cq)?;
        let sq_len = plan.rings.get();
        let cq_len = plan.rings.get();
        let sqe_len = plan.entries.get();
        let submit = Region::Submission;
        let complete = Region::Completion;
        Ok(Self {
            sq: Submit {
                axis: sq,
                head: Self::offset(submit, params.sq_off.head, num::NonZero::<u32>::MIN, sq_len)?,
                tail: Self::offset(submit, params.sq_off.tail, num::NonZero::<u32>::MIN, sq_len)?,
                flags: Self::offset(
                    submit,
                    params.sq_off.flags,
                    num::NonZero::<u32>::MIN,
                    sq_len,
                )?,
                entries: Self::offset(Region::Entries, 0, sq.entries, sqe_len)?,
            },
            cq: Complete {
                axis: cq,
                head: Self::offset(
                    complete,
                    params.cq_off.head,
                    num::NonZero::<u32>::MIN,
                    cq_len,
                )?,
                tail: Self::offset(
                    complete,
                    params.cq_off.tail,
                    num::NonZero::<u32>::MIN,
                    cq_len,
                )?,
                entries: Self::offset(complete, params.cq_off.cqes, cq.entries, cq_len)?,
            },
        })
    }

    fn axis(region: Region, actual: u32, expected: Axis) -> Result<Axis, Layout> {
        if actual != expected.entries.get() {
            return Err(Layout::Entries {
                region,
                actual: Self::count(actual),
            });
        }
        Ok(expected)
    }

    /// The ABI stride of `T`: nonzero because the assertion pins one sized item.
    fn stride<T>() -> num::NonZero<usize> {
        const { assert!(mem::size_of::<T>() > 0) };
        num::NonZero::<usize>::MIN.saturating_add(mem::size_of::<T>() - 1)
    }

    /// The byte extent of `entries` items of `stride`: nonzero because both
    /// factors are nonzero, so its callers carry the length proof instead of
    /// re-testing it.
    fn extent(
        region: Region,
        offset: u32,
        entries: num::NonZero<u32>,
        stride: num::NonZero<usize>,
    ) -> Result<num::NonZero<usize>, Layout> {
        let count = entries.get();
        let width = num::NonZero::<usize>::MIN.saturating_add(count as usize - 1);
        let Some(len) = width
            .checked_mul(stride)
            .and_then(|bytes| bytes.checked_add(offset as usize))
        else {
            return Err(Layout::Overflow {
                region,
                offset: Self::count(offset),
                entries: Self::count(count),
                stride: Self::size(stride.get()),
            });
        };
        Ok(len)
    }

    fn offset<T>(
        region: Region,
        offset: u32,
        count: num::NonZero<u32>,
        maximum: usize,
    ) -> Result<Offset<T>, Layout> {
        let end = Self::extent(region, offset, count, Self::stride::<T>())?.get();
        let bytes = end - offset as usize;
        let align = mem::align_of::<T>();
        let start = offset as usize;
        if !start.is_multiple_of(align) || end > maximum {
            return Err(Layout::Offset {
                region,
                offset: Self::count(offset),
                bytes: Self::size(bytes),
                maximum: Self::size(maximum),
                alignment: Self::size(align),
            });
        }
        Ok(Offset {
            value: offset,
            item: marker::PhantomData,
        })
    }
}

impl Setup {
    pub(crate) fn new(
        entries: num::NonZero<u32>,
        cqes: num::NonZero<u32>,
        taskrun: bool,
    ) -> Result<Self, Error> {
        use crate::reactor::ring::ffi;

        let mut flags = ffi::SETUP_SUBMIT_ALL
            | ffi::SETUP_SINGLE_ISSUER
            | ffi::SETUP_NO_SQARRAY
            | ffi::SETUP_CQSIZE
            | ffi::SETUP_NO_MMAP;
        if taskrun {
            flags |= ffi::SETUP_DEFER_TASKRUN | ffi::SETUP_TASKRUN_FLAG;
        }
        let plan = Plan::new(entries, cqes).map_err(Error::Layout)?;
        let sqes = ring::ffi::Map::new(Region::Entries, plan.entries)
            .map_err(|open| Self::open(open, None))?;
        let rings = match ring::ffi::Map::new(Region::Submission, plan.rings) {
            Ok(rings) => rings,
            Err(open) => return Err(Self::open(open, Some(sqes))),
        };
        let mut params = ring::ffi::Parameters {
            flags,
            cq_entries: cqes.get(),
            ..ring::ffi::Parameters::default()
        };
        params.sq_off.user_address = sqes.address();
        params.cq_off.user_address = rings.address();
        let maps = map::Set::single(rings, sqes);
        let fd = match ring::ffi::Fd::setup(entries, &mut params) {
            Ok(fd) => fd,
            Err(cause) => {
                return Err(Self::finish(None, Cause::Setup(cause), maps.try_close()));
            }
        };
        let geometry = match Geometry::new(&params, &plan) {
            Ok(geometry) => geometry,
            Err(cause) => {
                return Err(Self::finish(
                    Some(fd),
                    Cause::Layout(cause),
                    maps.try_close(),
                ));
            }
        };
        let mapped = Mapped::from_maps(maps, geometry);
        Ok(Self {
            fd,
            mapped,
            parameters: params,
        })
    }
}

impl Mapped {
    pub(crate) fn sq(&mut self) -> &mut sq::raw::State {
        &mut self.sq
    }

    pub(crate) fn completion(&mut self) -> &mut cq::raw::State {
        &mut self.completion
    }

    fn from_maps(maps: map::Set, geometry: Geometry) -> Self {
        // SAFETY: Geometry admitted every complete array and aligned field
        // against the live mappings, which are moved into this owner.
        let sq = unsafe { sq::raw::State::new(maps.rings(), maps.entries(), geometry.sq) };
        // SAFETY: the same proof covers the complete CQ array and its fields.
        let cq = unsafe { cq::raw::State::new(maps.rings(), geometry.cq) };
        Self {
            sq,
            completion: cq,
            maps,
        }
    }

    pub(crate) fn into_maps(self) -> map::Set {
        self.maps
    }
}

impl Setup {
    fn open(open: map::Open, previous: Option<ring::ffi::Map>) -> Error {
        match open {
            map::Open::Failure(failure) => Self::finish(
                None,
                Cause::Map(failure),
                previous.map_or(Ok(()), map::Closing::one),
            ),
            map::Open::Cleanup { failure, unmap } => Error::Cleanup(Cleanup::new(
                None,
                Cause::Map(failure),
                match previous {
                    Some(previous) => map::Closing::dirty_two(unmap, previous),
                    None => map::Closing::dirty(unmap),
                },
            )),
        }
    }

    fn finish(fd: Option<ring::ffi::Fd>, failure: Cause, close: Result<(), map::Closing>) -> Error {
        match close {
            Ok(()) => failure.into(),
            Err(maps) => Error::Cleanup(Cleanup::new(fd, failure, maps)),
        }
    }
}

const _: () = {
    assert!(
        mem::size_of::<Mapped>()
            == mem::size_of::<sq::raw::State>()
                + mem::size_of::<cq::raw::State>()
                + mem::size_of::<map::Set>()
    );
    assert!(mem::align_of::<Mapped>() == mem::align_of::<ring::ffi::Map>());
    assert!(mem::size_of::<Option<Mapped>>() == mem::size_of::<Mapped>());
};
