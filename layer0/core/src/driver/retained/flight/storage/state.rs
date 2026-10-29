use std::{cell, marker, mem, num, ptr};

use o3::{
    alloc::{self, tail},
    depot::arena::{linked::chain, slab},
};

use crate::driver::retained::flight::storage::{Echo, Slots, generation_max};

type Block<T, const INDEX_BITS: u32, const GENERATION_BITS: u32> = tail::Block<
    chain::Node<Header<T, INDEX_BITS, GENERATION_BITS>, State<T, INDEX_BITS, GENERATION_BITS>>,
    State<T, INDEX_BITS, GENERATION_BITS>,
>;

const ATTACHED: usize = 1;
const PENDING: usize = 2;
const BOUND: usize = 4;
const FLAGS: usize = ATTACHED | PENDING | BOUND;
const RETIRED: usize = ATTACHED | BOUND;
const LEASED: u64 = 1 << 63;
const EMPTY: u64 = 1 << 62;

#[repr(C, align(8))]
pub(super) struct State<T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32> {
    value: cell::Cell<mem::MaybeUninit<T>>,
    word: cell::UnsafeCell<Word<T, INDEX_BITS, GENERATION_BITS>>,
    index: u32,
    generation: cell::Cell<num::NonZero<u32>>,
}

union Word<T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32> {
    free: mem::ManuallyDrop<Option<Link<T, INDEX_BITS, GENERATION_BITS>>>,
    occupied: *mut Header<T, INDEX_BITS, GENERATION_BITS>,
}

/// Affine ownership of an entry in its allocation's intrusive free list.
#[repr(transparent)]
struct Link<T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32>(
    ptr::NonNull<State<T, INDEX_BITS, GENERATION_BITS>>,
);

/// Allocation owner for the three low state bits and the vacant-entry chain.
#[repr(align(8))]
pub(super) struct Header<T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32> {
    free: cell::Cell<Option<Link<T, INDEX_BITS, GENERATION_BITS>>>,
    /// At most 2^32 issued entries fit below the lease and empty-group flags.
    /// The count includes held entries until their permanent retirement.
    admission: cell::Cell<u64>,
}

#[repr(C)]
pub(super) struct Vacant<'a, T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32> {
    entry: &'a State<T, INDEX_BITS, GENERATION_BITS>,
    owner: &'a Header<T, INDEX_BITS, GENERATION_BITS>,
}

#[repr(transparent)]
pub(super) struct Live<'a, T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32> {
    entry: &'a State<T, INDEX_BITS, GENERATION_BITS>,
    invariant: marker::PhantomData<fn(&'a ()) -> &'a ()>,
}

#[repr(transparent)]
pub(super) struct Bound<'a, T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32> {
    live: Live<'a, T, INDEX_BITS, GENERATION_BITS>,
}

impl<T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32>
    State<T, INDEX_BITS, GENERATION_BITS>
{
    const fn new(index: u32) -> Self {
        Self {
            value: cell::Cell::new(mem::MaybeUninit::uninit()),
            word: cell::UnsafeCell::new(Word {
                free: mem::ManuallyDrop::new(None),
            }),
            index,
            generation: cell::Cell::new(num::NonZero::<u32>::MIN),
        }
    }

    fn pointer(&self) -> *mut Header<T, INDEX_BITS, GENERATION_BITS> {
        // SAFETY: both union fields have the nullable-pointer representation.
        unsafe { (*self.word.get()).occupied }
    }

    fn set(&self, pointer: *mut Header<T, INDEX_BITS, GENERATION_BITS>) {
        // SAFETY: the live capability owns this occupied union variant.
        unsafe { (*self.word.get()).occupied = pointer };
    }

    pub(super) fn is_pending(&self) -> bool {
        self.pointer().addr() & PENDING != 0
    }

    pub(super) fn finish(&self) {
        if let Some(bound) = self.bind(self.generation.get()) {
            bound.finish();
        }
    }

    pub(super) fn bind(
        &self,
        expected: num::NonZero<u32>,
    ) -> Option<Bound<'_, T, INDEX_BITS, GENERATION_BITS>> {
        let pointer = self.pointer();
        let address = pointer.addr();
        if address & (PENDING | BOUND) != PENDING || self.generation.get() != expected {
            return None;
        }
        self.set(pointer.map_addr(|address| address | BOUND));
        Some(Bound {
            live: Live {
                entry: self,
                invariant: marker::PhantomData,
            },
        })
    }
}

impl<T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32>
    Header<T, INDEX_BITS, GENERATION_BITS>
{
    pub(super) fn try_block(
        extent: usize,
        seed: u32,
    ) -> Result<Block<T, INDEX_BITS, GENERATION_BITS>, alloc::Error> {
        let header = Self {
            free: cell::Cell::new(None),
            admission: cell::Cell::new(0),
        };
        let memory = tail::Plan::try_new(extent)?.try_allocate()?;
        Ok(memory.fill(chain::Node::new(header), |_, index| {
            State::new(seed + index as u32)
        }))
    }

    pub(super) fn select(
        groups: &chain::List<Self, State<T, INDEX_BITS, GENERATION_BITS>>,
        capacity: usize,
    ) -> Option<Slots<'_, T, INDEX_BITS, GENERATION_BITS>> {
        let mut selected = None;
        let mut best = usize::MAX;
        'groups: for group in groups.iter() {
            if group.len() < capacity || group.header().admission.get() & LEASED != 0 {
                continue;
            }
            let mut count = 0;
            for entry in group.as_slice() {
                let flags = entry.pointer().addr() & FLAGS;
                if flags == 0 {
                    count += 1;
                } else if flags != RETIRED {
                    continue 'groups;
                }
            }
            if count >= capacity && count < best {
                selected = Some(group);
                best = count;
                if count == capacity {
                    break;
                }
            }
        }
        Some(Self::activate(selected?, capacity))
    }

    fn activate(
        group: chain::Link<'_, Self, State<T, INDEX_BITS, GENERATION_BITS>>,
        capacity: usize,
    ) -> Slots<'_, T, INDEX_BITS, GENERATION_BITS> {
        let header = group.header();
        let state = if capacity == 0 {
            EMPTY
        } else {
            capacity as u64
        };
        header.admission.set(LEASED | state);
        header.free.set(None);
        let mut left = capacity;
        for entry in group.as_slice().iter().rev() {
            if left == 0 {
                break;
            }
            if entry.pointer().addr() & FLAGS == 0 {
                Vacant {
                    entry,
                    owner: header,
                }
                .release();
                left -= 1;
            }
        }
        Slots { group }
    }

    pub(super) fn publish(
        groups: &chain::List<Self, State<T, INDEX_BITS, GENERATION_BITS>>,
        block: tail::Block<
            chain::Node<Self, State<T, INDEX_BITS, GENERATION_BITS>>,
            State<T, INDEX_BITS, GENERATION_BITS>,
        >,
        capacity: usize,
    ) -> Slots<'_, T, INDEX_BITS, GENERATION_BITS> {
        Self::activate(groups.push(block), capacity)
    }

    pub(super) fn try_vacancy(
        &self,
    ) -> Result<Vacant<'_, T, INDEX_BITS, GENERATION_BITS>, slab::Vacancy> {
        let Some(Link(pointer)) = self.free.take() else {
            return Err(if self.admission.get() & !LEASED == 0 {
                slab::Vacancy::IdentityExhausted
            } else {
                slab::Vacancy::Full
            });
        };
        // SAFETY: the affine link owns this vacant entry in this header's block.
        let entry = unsafe { pointer.as_ref() };
        // SAFETY: taking the link consumes the sole authority to its free variant.
        let next = unsafe { mem::ManuallyDrop::take(&mut (*entry.word.get()).free) };
        self.free.set(next);
        Ok(Vacant { entry, owner: self })
    }

    pub(super) fn release(&self) {
        self.admission.set(self.admission.get() & !LEASED);
    }
}

impl<'a, T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32>
    Vacant<'a, T, INDEX_BITS, GENERATION_BITS>
{
    pub(super) fn occupy(self, value: T) -> Live<'a, T, INDEX_BITS, GENERATION_BITS> {
        self.entry.value.set(mem::MaybeUninit::new(value));
        self.entry.set(
            ptr::from_ref(self.owner)
                .cast_mut()
                .map_addr(|address| address | ATTACHED | PENDING),
        );
        Live {
            entry: self.entry,
            invariant: marker::PhantomData,
        }
    }

    pub(super) fn release(self) {
        // SAFETY: this vacancy exclusively owns the free variant and its owner.
        unsafe {
            *self.entry.word.get() = Word {
                free: mem::ManuallyDrop::new(self.owner.free.take()),
            };
        }
        self.owner
            .free
            .set(Some(Link(ptr::NonNull::from(self.entry))));
    }
}

impl<'a, T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32>
    Live<'a, T, INDEX_BITS, GENERATION_BITS>
{
    pub(super) fn is_pending(&self) -> bool {
        self.entry.is_pending()
    }

    pub(super) fn value(&self) -> T {
        // SAFETY: this capability retains occupancy until its consuming release.
        unsafe { self.entry.value.get().assume_init() }
    }

    pub(super) fn echo(&self) -> Echo<T, INDEX_BITS, GENERATION_BITS> {
        Echo::from_parts(self.entry.index, self.entry.generation.get())
    }

    pub(super) fn detach(self) {
        let pointer = self.entry.pointer();
        if pointer.addr() & FLAGS == ATTACHED {
            self.recycle();
        } else {
            self.entry
                .set(pointer.map_addr(|address| address & !ATTACHED));
        }
    }

    pub(super) fn release(self) {
        let pointer = self.entry.pointer();
        if pointer.addr() & BOUND != 0 {
            self.entry
                .set(pointer.map_addr(|address| address & !(ATTACHED | PENDING)));
        } else {
            self.recycle();
        }
    }

    /// Returns this issued slot or retires its counted identity exactly once.
    fn recycle(self) {
        let owner = self.entry.pointer().map_addr(|address| address & !FLAGS);
        // SAFETY: the live capability retains its aligned allocation owner for 'a.
        let owner: &'a Header<T, INDEX_BITS, GENERATION_BITS> = unsafe { &*owner };
        let next = self.entry.generation.get().checked_add(1);
        let Some(next) = next.filter(|next| next.get() <= generation_max::<GENERATION_BITS>())
        else {
            self.entry.set(ptr::without_provenance_mut(RETIRED));
            owner.admission.set(owner.admission.get() - 1);
            return;
        };
        self.entry.generation.set(next);
        Vacant {
            entry: self.entry,
            owner,
        }
        .release();
    }
}

impl<T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32>
    Bound<'_, T, INDEX_BITS, GENERATION_BITS>
{
    pub(super) fn echo(&self) -> Echo<T, INDEX_BITS, GENERATION_BITS> {
        self.live.echo()
    }

    pub(super) fn value(&self) -> T {
        self.live.value()
    }

    pub(super) fn complete(self) -> T {
        let value = self.value();
        self.finish();
        value
    }

    pub(super) fn finish(self) {
        let live = self.live;
        let pointer = live.entry.pointer();
        if pointer.addr() & ATTACHED == 0 {
            live.recycle();
        } else {
            live.entry
                .set(pointer.map_addr(|address| address & !(PENDING | BOUND)));
        }
    }

    pub(super) fn retain(self) {
        let pointer = self.live.entry.pointer();
        if pointer.addr() & PENDING == 0 {
            self.live.recycle();
        } else {
            self.live
                .entry
                .set(pointer.map_addr(|address| address & !BOUND));
        }
    }
}

const _: () = {
    assert!(mem::size_of::<State<u64, 32, 32>>() == 3 * mem::size_of::<usize>());
    assert!(mem::size_of::<Header<u64, 32, 32>>() == 2 * mem::size_of::<usize>());
    assert!(mem::align_of::<State<u64, 32, 32>>() == 8);
    assert!(mem::align_of::<Header<u64, 32, 32>>() == 8);
    assert!(mem::size_of::<Vacant<'static, u64, 32, 32>>() == 2 * mem::size_of::<usize>());
    assert!(mem::size_of::<Live<'static, u64, 32, 32>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Option<Live<'static, u64, 32, 32>>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Bound<'static, u64, 32, 32>>() == mem::size_of::<usize>());
};
