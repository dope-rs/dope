//! Reusable slots for externally echoed flight completions.

use std::{marker, mem::size_of};

pub use builder::Builder;
pub use echo::Echo;
use o3::{
    alloc::tail,
    depot::arena::{linked::chain, slab},
    mem::{self, Return, lease},
};
pub use prepared::Prepared;

use crate::io::event::batch::Flow;

mod builder;
mod echo;
mod prepared;
pub(in crate::driver::retained::flight) mod raw;
mod state;

const fn is_layout_valid<const INDEX_BITS: u32, const GENERATION_BITS: u32>() -> bool {
    INDEX_BITS != 0
        && INDEX_BITS <= u32::BITS
        && GENERATION_BITS != 0
        && GENERATION_BITS <= u32::BITS
        && INDEX_BITS + GENERATION_BITS <= u64::BITS
}

const fn generation_max<const GENERATION_BITS: u32>() -> u32 {
    if GENERATION_BITS == 0 {
        0
    } else if GENERATION_BITS >= u32::BITS {
        u32::MAX
    } else {
        (1u32 << GENERATION_BITS) - 1
    }
}

const fn index_limit<const INDEX_BITS: u32>() -> usize {
    if INDEX_BITS == 0 {
        0
    } else if INDEX_BITS >= usize::BITS {
        usize::MAX
    } else {
        1usize << INDEX_BITS
    }
}

struct Release<T, B = ()>(T, B);
struct Detach<T>(T);

/// Rust ownership of one externally completing flight.
/// Dropping the lease preserves the slot until its external completion arrives.
#[must_use = "a live flight must reach terminal completion or be detached"]
#[repr(transparent)]
pub struct Lease<'owner, T: Copy, const INDEX_BITS: u32 = 32, const GENERATION_BITS: u32 = 32> {
    live: mem::Lease<Detach<state::Live<'owner, T, INDEX_BITS, GENERATION_BITS>>, lease::Open>,
}

impl<'owner, T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32>
    Lease<'owner, T, INDEX_BITS, GENERATION_BITS>
{
    pub fn is_pending(&self) -> bool {
        self.live.sink().0.is_pending()
    }

    pub fn echo(&self) -> Echo<T, INDEX_BITS, GENERATION_BITS> {
        self.live.sink().0.echo()
    }

    pub fn value(&self) -> T {
        self.live.sink().0.value()
    }
}

impl<T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32> Return
    for Detach<state::Live<'_, T, INDEX_BITS, GENERATION_BITS>>
{
    type Item = ();

    fn return_item(self, (): Self::Item) {
        self.0.detach();
    }
}

impl<'owner, T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32> Return
    for Release<
        state::Vacant<'owner, T, INDEX_BITS, GENERATION_BITS>,
        marker::PhantomData<&Slots<'owner, T, INDEX_BITS, GENERATION_BITS>>,
    >
{
    type Item = ();

    fn return_item(self, (): Self::Item) {
        self.0.release();
    }
}

impl<T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32> Return
    for Release<state::Live<'_, T, INDEX_BITS, GENERATION_BITS>>
{
    type Item = ();

    fn return_item(self, (): Self::Item) {
        self.0.release();
    }
}

const _: () = {
    assert!(size_of::<Complete<'static, ()>>() == size_of::<usize>());
    assert!(size_of::<Option<Complete<'static, ()>>>() == size_of::<usize>());
    assert!(size_of::<Reserved<'static, ()>>() == size_of::<usize>());
    assert!(size_of::<Option<Reserved<'static, ()>>>() == size_of::<usize>());
    assert!(size_of::<Lease<'static, ()>>() == size_of::<usize>());
    assert!(size_of::<Option<Lease<'static, ()>>>() == size_of::<usize>());
    assert!(size_of::<Vacant<'static, 'static, ()>>() == 2 * size_of::<usize>());
    assert!(size_of::<Option<Vacant<'static, 'static, ()>>>() == 2 * size_of::<usize>());
};

struct Retain<T>(T);

/// One matched external completion tied to its owning arena.
#[must_use = "a bound completion must be applied"]
#[repr(transparent)]
pub struct Complete<'owner, T: Copy, const INDEX_BITS: u32 = 32, const GENERATION_BITS: u32 = 32> {
    bound: mem::Lease<Retain<state::Bound<'owner, T, INDEX_BITS, GENERATION_BITS>>, lease::Open>,
}

impl<'owner, T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32>
    Complete<'owner, T, INDEX_BITS, GENERATION_BITS>
{
    pub(in crate::driver::retained::flight::storage) fn new(
        bound: state::Bound<'owner, T, INDEX_BITS, GENERATION_BITS>,
    ) -> Self {
        Self {
            bound: mem::Lease::open(Retain(bound), ()),
        }
    }

    pub fn echo(&self) -> Echo<T, INDEX_BITS, GENERATION_BITS> {
        self.bound.sink().0.echo()
    }

    pub fn value(&self) -> T {
        self.bound.sink().0.value()
    }

    pub fn complete(self) -> T {
        let (Retain(bound), ()) = self.bound.into_parts();
        bound.complete()
    }

    /// Releases only this completion loan until the operation's last packet.
    pub fn settle(self, flow: Flow) -> T {
        match flow {
            Flow::Last => self.complete(),
            Flow::More => self.value(),
        }
    }

    pub fn finish(self) {
        let (Retain(bound), ()) = self.bound.into_parts();
        bound.finish();
    }
}

impl<T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32> Return
    for Retain<state::Bound<'_, T, INDEX_BITS, GENERATION_BITS>>
{
    type Item = ();

    fn return_item(self, (): Self::Item) {
        self.0.retain();
    }
}

/// A flight slot that rolls back unless committed.
#[must_use = "a reserved flight must be committed or released"]
#[repr(transparent)]
pub struct Reserved<'owner, T: Copy, const INDEX_BITS: u32 = 32, const GENERATION_BITS: u32 = 32> {
    live: mem::Lease<Release<state::Live<'owner, T, INDEX_BITS, GENERATION_BITS>>, lease::Open>,
}

impl<'owner, T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32>
    Reserved<'owner, T, INDEX_BITS, GENERATION_BITS>
{
    pub fn echo(&self) -> Echo<T, INDEX_BITS, GENERATION_BITS> {
        self.live.sink().0.echo()
    }

    pub fn commit(self) -> Lease<'owner, T, INDEX_BITS, GENERATION_BITS> {
        let (Release(live, ()), ()) = self.live.into_parts();
        Lease {
            live: mem::Lease::open(Detach(live), ()),
        }
    }
}

/// A leased group of reusable flight slots.
#[repr(transparent)]
pub struct Slots<'owner, T: Copy, const INDEX_BITS: u32 = 32, const GENERATION_BITS: u32 = 32> {
    pub(in crate::driver::retained::flight::storage) group: chain::Link<
        'owner,
        state::Header<T, INDEX_BITS, GENERATION_BITS>,
        state::State<T, INDEX_BITS, GENERATION_BITS>,
    >,
}

impl<'owner, T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32>
    Slots<'owner, T, INDEX_BITS, GENERATION_BITS>
{
    /// Retains one issued slot, distinguishing temporary occupancy from the
    /// permanent retirement of every issued identity in this group.
    pub fn try_vacancy(
        &self,
    ) -> Result<Vacant<'_, 'owner, T, INDEX_BITS, GENERATION_BITS>, slab::Vacancy> {
        Ok(Vacant {
            vacant: mem::Lease::open(
                Release(self.group.header().try_vacancy()?, marker::PhantomData),
                (),
            ),
        })
    }
}

impl<T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32> Drop
    for Slots<'_, T, INDEX_BITS, GENERATION_BITS>
{
    fn drop(&mut self) {
        self.group.header().release();
    }
}

/// Exclusive vacant entry returned to its group when dropped.
///
/// The group remains borrowed even when the vacancy is only implicitly dropped.
#[must_use = "a vacancy must be reserved or released"]
#[repr(transparent)]
pub struct Vacant<
    'slot,
    'owner,
    T: Copy,
    const INDEX_BITS: u32 = 32,
    const GENERATION_BITS: u32 = 32,
> {
    vacant: mem::Lease<
        Release<
            state::Vacant<'owner, T, INDEX_BITS, GENERATION_BITS>,
            marker::PhantomData<&'slot Slots<'owner, T, INDEX_BITS, GENERATION_BITS>>,
        >,
        lease::Open,
    >,
}

impl<'owner, T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32>
    Vacant<'_, 'owner, T, INDEX_BITS, GENERATION_BITS>
{
    pub fn reserve(self, value: T) -> Reserved<'owner, T, INDEX_BITS, GENERATION_BITS> {
        let (Release(vacant, _), ()) = self.vacant.into_parts();
        Reserved {
            live: mem::Lease::open(Release(vacant.occupy(value), ()), ()),
        }
    }
}

/// Reusable externally completed flights with packed index/generation echoes.
pub struct Flights<T: Copy, const INDEX_BITS: u32 = 32, const GENERATION_BITS: u32 = 32> {
    groups: chain::List<
        state::Header<T, INDEX_BITS, GENERATION_BITS>,
        state::State<T, INDEX_BITS, GENERATION_BITS>,
    >,
    directory: raw::Directory<T, INDEX_BITS, GENERATION_BITS>,
}

impl<T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32>
    Flights<T, INDEX_BITS, GENERATION_BITS>
{
    pub const fn new() -> Self {
        Self {
            groups: chain::List::new(),
            directory: raw::Directory::new(),
        }
    }

    /// Exclusively owns preparation while completed groups borrow this arena.
    pub fn builder(&mut self) -> Builder<'_, T, INDEX_BITS, GENERATION_BITS> {
        Builder { arena: self }
    }

    pub fn drain(&self) -> Drain<'_, T, INDEX_BITS, GENERATION_BITS> {
        Drain { arena: self }
    }

    pub(in crate::driver::retained::flight) fn is_pending(&self) -> bool {
        self.groups
            .iter()
            .any(|group| group.as_slice().iter().any(state::State::is_pending))
    }

    pub(in crate::driver::retained::flight) fn finish(&self) {
        for group in self.groups.iter() {
            for entry in group.as_slice() {
                entry.finish();
            }
        }
    }
}

/// Borrowed authority to apply external completion echoes.
#[repr(transparent)]
pub struct Drain<'arena, T: Copy, const INDEX_BITS: u32 = 32, const GENERATION_BITS: u32 = 32> {
    arena: &'arena Flights<T, INDEX_BITS, GENERATION_BITS>,
}

impl<'arena, T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32>
    Drain<'arena, T, INDEX_BITS, GENERATION_BITS>
{
    pub fn bind(
        &self,
        echo: Echo<T, INDEX_BITS, GENERATION_BITS>,
    ) -> Option<Complete<'arena, T, INDEX_BITS, GENERATION_BITS>> {
        let entry = self.entry(echo.index())?;
        Some(Complete::new(entry.bind(echo.generation())?))
    }
}

enum Preparation<'owner, T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32> {
    Reuse(Slots<'owner, T, INDEX_BITS, GENERATION_BITS>),
    New {
        block: tail::Block<
            chain::Node<
                state::Header<T, INDEX_BITS, GENERATION_BITS>,
                state::State<T, INDEX_BITS, GENERATION_BITS>,
            >,
            state::State<T, INDEX_BITS, GENERATION_BITS>,
        >,
        capacity: usize,
    },
}

impl<'arena, T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32>
    Drain<'arena, T, INDEX_BITS, GENERATION_BITS>
{
    fn entry(&self, index: u32) -> Option<&'arena state::State<T, INDEX_BITS, GENERATION_BITS>> {
        let pointer = self.arena.directory.read().get(index as usize)?;
        // SAFETY: every directory pointer names an entry owned by this arena.
        Some(unsafe { pointer.as_ref() })
    }
}

impl<T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32> Default
    for Flights<T, INDEX_BITS, GENERATION_BITS>
{
    fn default() -> Self {
        Self::new()
    }
}
