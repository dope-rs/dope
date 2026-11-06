use std::{cell, mem, net, pin};

use dope_sys::io::socket::addr::{Length, Storage};
use o3::base::cell::region;

use crate::{
    driver,
    driver::{
        life::finish,
        result,
        retained::{accept, arm, flight},
        route,
        route::kind,
        schedule,
    },
    io::{
        event::{batch::Flow, net::accept::Failure},
        fd::domain::retire,
        socket,
    },
};

pub mod raw;

/// Current descriptor-retirement barrier for admitting a new accept.
#[derive(Debug)]
pub enum Readiness {
    Ready,
    Pending,
    Inflight,
}

impl From<driver::Reference<'_>> for Readiness {
    /// Query again after driver mutations; this snapshot is not an admission permit.
    fn from(driver: driver::Reference<'_>) -> Self {
        let retired = driver.descriptors().retirement();
        if retired.has_pending() {
            Self::Pending
        } else if retired.has_inflight() {
            Self::Inflight
        } else {
            Self::Ready
        }
    }
}

impl Readiness {
    pub fn progress<'a>(
        self,
        region: &region::Token<region::Identity<'a>>,
    ) -> schedule::Progress<'a> {
        match self {
            Self::Ready | Self::Pending => schedule::Progress::Runnable,
            Self::Inflight => schedule::Progress::waiting(region),
        }
    }
}

#[pin_project::pin_project(!Unpin)]
pub struct Lane<'d, Tag>
where
    Tag: route::Tag<Class = kind::Accept>,
{
    state: arm::State<'d, Tag>,
    addr: cell::UnsafeCell<Storage<Length>>,
    closed: retire::Completed,
}

impl<'d, Tag> Lane<'d, Tag>
where
    Tag: route::Tag<Class = kind::Accept>,
{
    pub fn new(target: route::Target<'d, Tag>) -> Self {
        Self {
            state: arm::State::new(target),
            addr: cell::UnsafeCell::new(Storage::<Length>::empty()),
            closed: retire::Completed::EMPTY,
        }
    }
}

/// Pinned accept lanes and their exact in-flight count share one owner.
///
pub struct Slots<'d, Tag>
where
    Tag: route::Tag<Class = kind::Accept>,
{
    lanes: pin::Pin<Box<[Lane<'d, Tag>]>>,
    active: usize,
}

impl<'d, Tag> Slots<'d, Tag>
where
    Tag: route::Tag<Class = kind::Accept>,
{
    pub fn new(lanes: Box<[Lane<'d, Tag>]>) -> Self {
        Self {
            lanes: Box::into_pin(lanes),
            active: 0,
        }
    }

    pub fn get_mut(&mut self, index: usize) -> Option<Entry<'_, 'd, Tag>> {
        // SAFETY: the selected lane remains pinned in this owned allocation.
        let lane = unsafe { self.lanes.as_mut().get_unchecked_mut() }.get_mut(index)?;
        Some(Entry {
            lane: unsafe { pin::Pin::new_unchecked(lane) },
            active: &mut self.active,
        })
    }

    fn iter_mut(
        &mut self,
    ) -> impl DoubleEndedIterator<Item = pin::Pin<&mut Lane<'d, Tag>>> + ExactSizeIterator + '_
    {
        // SAFETY: the boxed slice keeps every lane at a stable address, and
        // the iterator exposes each exclusive borrow only as pinned.
        unsafe { self.lanes.as_mut().get_unchecked_mut() }
            .iter_mut()
            .map(|lane| unsafe { pin::Pin::new_unchecked(lane) })
    }

    pub fn is_empty(&self) -> bool {
        self.lanes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.lanes.len()
    }

    /// Returns the number of submitted accepts awaiting terminal retirement.
    pub const fn active(&self) -> usize {
        self.active
    }

    pub fn stop(&mut self, driver: &mut driver::Context<'_, 'd>) {
        for lane in self.iter_mut() {
            lane.project().state.stop(driver);
        }
    }

    pub fn progress<'a>(
        &self,
        region: &region::Token<region::Identity<'a>>,
    ) -> schedule::Progress<'a> {
        self.lanes
            .iter()
            .fold(schedule::Progress::Quiescent, |progress, lane| {
                progress.reduce(lane.state.progress(region))
            })
    }

    pub fn finish(&mut self, proof: &mut finish::Finish<'_, 'd>) {
        for lane in self.iter_mut() {
            lane.project().state.finish(proof);
        }
        self.active = 0;
    }
}

/// An exclusive lane selection retains its owning count through the transition.
///
///
pub struct Entry<'a, 'd, Tag>
where
    Tag: route::Tag<Class = kind::Accept>,
{
    lane: pin::Pin<&'a mut Lane<'d, Tag>>,
    active: &'a mut usize,
}

impl<'a, 'd, Tag> Entry<'a, 'd, Tag>
where
    Tag: route::Tag<Class = kind::Accept>,
{
    pub fn begin(mut self) -> Option<accept::raw::Arming<'a, 'd, Tag>> {
        if !self.lane.as_mut().project().state.start() {
            return None;
        }
        Some(accept::raw::Arming::new(self))
    }

    /// Retires an operation while borrowing its owner's active count.
    /// The matching lane must have reached native terminal completion.
    /// Empty owners and pending flights cannot grant terminal access.
    pub fn retire(mut self, token: route::Erased<'d, kind::Accept>) -> Retire<'a, 'd, Tag> {
        let lane = self.lane.as_mut().project();
        let retired = match lane.state.complete(token, Flow::Last) {
            arm::Disposition::Discard => return Retire::Stale,
            arm::Disposition::Deliver | arm::Disposition::Probe => {
                Retired::Terminal(Terminal { lane: self.lane })
            }
            arm::Disposition::Retiring => Retired::Cancelled,
        };
        // A successful retirement consumes one live lane counted by this owner.
        *self.active -= 1;
        Retire::Done(retired)
    }

    /// Commits a previously idle lane and its owning count together.
    /// The count cannot exceed the allocated lane slice's length.
    fn apply(
        &mut self,
        result: Result<flight::Flight<'d, kind::Accept>, result::SubmitError>,
        closed: retire::Completed,
    ) -> arm::Submission {
        let lane = self.lane.as_mut().project();
        let result = lane.state.apply(result);
        if let arm::Submission::Armed = result {
            *lane.closed = closed;
            // Entry owns one distinct idle lane in this count's owned slice.
            *self.active += 1;
        }
        result
    }
}

#[must_use]
pub enum Retire<'a, 'd, Tag>
where
    Tag: route::Tag<Class = kind::Accept>,
{
    Stale,
    Done(Retired<'a, 'd, Tag>),
}

#[must_use]
pub enum Retired<'a, 'd, Tag>
where
    Tag: route::Tag<Class = kind::Accept>,
{
    Terminal(Terminal<'a, 'd, Tag>),
    Cancelled,
}

#[must_use]
pub struct Terminal<'a, 'd, Tag>
where
    Tag: route::Tag<Class = kind::Accept>,
{
    lane: pin::Pin<&'a mut Lane<'d, Tag>>,
}

impl<'d, Tag> Terminal<'_, 'd, Tag>
where
    Tag: route::Tag<Class = kind::Accept>,
{
    /// Preserves completed native capacity returns even when their CQE precedes this error.
    pub fn can_defer(&self, failure: &Failure, driver: &mut driver::Context<'_, 'd>) -> bool {
        failure.can_defer(driver)
            || (matches!(failure, Failure::Syscall(_))
                && failure.is_resource()
                && self.lane.closed != driver.driver_ref().descriptors().retirement().completed())
    }

    pub fn into_addr(self) -> Result<net::SocketAddr, socket::Error> {
        let storage = self.lane.project().addr.get_mut();
        storage.to_std().map_err(socket::Error::from)
    }
}

const _: () = {
    type Tag = route::KeyTag<1, kind::Accept>;
    const fn layout<'a, 'd: 'a>() {
        assert!(
            mem::size_of::<Lane<'d, Tag>>()
                == mem::size_of::<(arm::State<'d, Tag>, Storage<Length>, retire::Completed)>()
        );
        assert!(
            mem::align_of::<Lane<'d, Tag>>()
                == mem::align_of::<(arm::State<'d, Tag>, Storage<Length>, retire::Completed)>()
        );
        assert!(
            mem::size_of::<Slots<'d, Tag>>()
                == mem::size_of::<(pin::Pin<Box<[Lane<'d, Tag>]>>, usize)>()
        );
        assert!(
            mem::align_of::<Slots<'d, Tag>>() == mem::align_of::<pin::Pin<Box<[Lane<'d, Tag>]>>>()
        );
        assert!(mem::size_of::<Terminal<'a, 'd, Tag>>() == mem::size_of::<usize>());
        assert!(mem::size_of::<Entry<'a, 'd, Tag>>() == 2 * mem::size_of::<usize>());
        assert!(mem::size_of::<Option<Entry<'a, 'd, Tag>>>() == 2 * mem::size_of::<usize>());
        assert!(mem::size_of::<accept::raw::Arming<'a, 'd, Tag>>() == 2 * mem::size_of::<usize>());
    }
    layout();
};
