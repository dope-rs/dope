use core::pin::Pin;
use core::task::Poll;

use dope::driver::ready::{CompletionRegistrarWithRegion, CompletionWaker};
use o3::cell::RegionToken;
use o3::collections::{CellQueue, Slab, SlabKey};

use crate::local::{LocalCell, LocalContext};
use crate::{Context, Fiber};

enum NotifyTag {}
type Key = SlabKey<NotifyTag>;

struct Slot<'d> {
    notified: bool,
    wake: Option<CompletionWaker<'d>>,
}

/// Fixed-capacity, generation-checked local notification storage.
///
/// Receivers own registrations. Dropping a receiver queues retirement, and
/// every operation drains retirements before it can observe or invoke a
/// retained task wake target.
pub struct NotifyArena<'d> {
    slots: LocalCell<'d, Slab<Slot<'d>, NotifyTag>>,
    retired: CellQueue<Key>,
}

impl<'d> NotifyArena<'d> {
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "notify arena capacity must be positive");
        Self {
            slots: LocalCell::new(Slab::with_capacity(capacity)),
            retired: CellQueue::with_capacity(capacity),
        }
    }

    fn drain_retired(&self, context: &mut LocalContext<'_, 'd>) {
        while let Some(key) = self.retired.pop_front() {
            self.slots.write_with(context, |slots| {
                slots.remove(key);
            });
        }
    }

    pub fn pair(
        &'d self,
        context: &mut LocalContext<'_, 'd>,
    ) -> Option<(NotifySender<'d>, NotifyReceiver<'d>)> {
        self.drain_retired(context);
        let key = self.slots.write_with(context, |slots| {
            slots
                .insert(Slot {
                    notified: false,
                    wake: None,
                })
                .ok()
        })?;
        Some((
            NotifySender { arena: self, key },
            NotifyReceiver {
                arena: self,
                key: Some(key),
            },
        ))
    }

    fn notify(&self, context: &mut LocalContext<'_, 'd>, key: Key) -> bool {
        self.drain_retired(context);
        let wake = self.slots.write_with(context, |slots| {
            let slot = slots.get_mut(key)?;
            slot.notified = true;
            Some(slot.wake.take())
        });
        let Some(wake) = wake else {
            return false;
        };
        if let Some(wake) = wake {
            wake.wake();
        }
        true
    }

    fn register(
        &self,
        context: &mut LocalContext<'_, 'd>,
        key: Key,
        wake: CompletionWaker<'d>,
    ) -> Poll<()> {
        self.drain_retired(context);
        self.slots.write_with(context, |slots| {
            let Some(slot) = slots.get_mut(key) else {
                return Poll::Ready(());
            };
            if slot.notified {
                slot.notified = false;
                slot.wake = None;
                Poll::Ready(())
            } else {
                slot.wake = Some(wake);
                Poll::Pending
            }
        })
    }

    fn retire(&self, key: Key) {
        if self.retired.push_back(key).is_err() {
            // At most one receiver exists for each live slot, so overflow
            // means the ownership invariant was violated. Panicking from a
            // destructor could continue unwinding through retained pointers.
            std::process::abort();
        }
    }
}

#[derive(Clone, Copy)]
pub struct NotifySender<'d> {
    arena: &'d NotifyArena<'d>,
    key: Key,
}

impl<'d> NotifySender<'d> {
    pub fn notify(self, context: &mut LocalContext<'_, 'd>) -> bool {
        self.arena.notify(context, self.key)
    }
}

pub struct NotifyReceiver<'d> {
    arena: &'d NotifyArena<'d>,
    key: Option<Key>,
}

impl<'d> NotifyReceiver<'d> {
    pub fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<()> {
        cx.register_completion_with_region(self)
    }
}

// SAFETY: NotifyReceiver::drop queues its unique generation key for
// retirement. pair, notify, and register drain that retirement before they
// can observe or invoke the retained completion handle.
unsafe impl<'d> CompletionRegistrarWithRegion<'d> for Pin<&mut NotifyReceiver<'d>> {
    type Output = Poll<()>;

    fn register(self, wake: CompletionWaker<'d>, region: &mut RegionToken<'d>) -> Self::Output {
        let this = self.get_mut();
        let Some(key) = this.key else {
            return Poll::Ready(());
        };
        let mut context = LocalContext::from_region(region);
        this.arena.register(&mut context, key, wake)
    }
}

impl<'d> Fiber<'d> for NotifyReceiver<'d> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        NotifyReceiver::poll(self, cx)
    }
}

impl Drop for NotifyReceiver<'_> {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.arena.retire(key);
        }
    }
}
