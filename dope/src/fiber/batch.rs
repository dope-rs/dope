use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

const BATCH_WINDOW: usize = 32;

pub struct Lazy<F, Fut> {
    state: LazyState<F, Fut>,
}

enum LazyState<F, Fut> {
    Pending(Option<F>),
    Active(Fut),
}

impl<F, Fut> Lazy<F, Fut> {
    pub fn new(f: F) -> Self {
        Self {
            state: LazyState::Pending(Some(f)),
        }
    }
}

impl<F, Fut> Future for Lazy<F, Fut>
where
    F: FnOnce() -> Fut,
    Fut: Future,
{
    type Output = Fut::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Fut::Output> {
        // SAFETY: structural pin — `state` and its `Active` future are never moved out.
        let me = unsafe { self.get_unchecked_mut() };
        if let LazyState::Pending(f) = &mut me.state {
            let f = f.take().expect("Lazy polled after completion");
            me.state = LazyState::Active(f());
        }
        let LazyState::Active(fut) = &mut me.state else {
            unreachable!("Lazy state is Active after dispatch");
        };
        // SAFETY: `fut` lives in the pinned `me`; re-pinned in place, never moved.
        unsafe { Pin::new_unchecked(fut) }.poll(cx)
    }
}

enum SubSlot<F: Future> {
    Idle(F),
    Live(F),
    Done(Option<F::Output>),
}

pub struct Batch<F: Future> {
    slots: Vec<SubSlot<F>>,
    window: usize,
    in_flight: usize,
    next_idle: usize,
    remaining: usize,
}

impl<F: Future> Batch<F> {
    pub fn new(futures: Vec<F>) -> Self {
        Self::with_window(futures, BATCH_WINDOW)
    }

    fn with_window(futures: Vec<F>, window: usize) -> Self {
        let n = futures.len();
        let slots = futures.into_iter().map(SubSlot::Idle).collect();
        Self {
            slots,
            window: window.max(1),
            in_flight: 0,
            next_idle: 0,
            remaining: n,
        }
    }

    fn admit(&mut self) -> bool {
        let mut admitted = false;
        while self.in_flight < self.window && self.next_idle < self.slots.len() {
            let i = self.next_idle;
            self.next_idle += 1;
            if matches!(self.slots[i], SubSlot::Idle(_)) {
                let SubSlot::Idle(f) = std::mem::replace(&mut self.slots[i], SubSlot::Done(None))
                else {
                    unreachable!()
                };
                self.slots[i] = SubSlot::Live(f);
                self.in_flight += 1;
                admitted = true;
            }
        }
        admitted
    }

    fn collect(slots: &mut [SubSlot<F>]) -> Vec<F::Output> {
        slots
            .iter_mut()
            .map(|s| match s {
                SubSlot::Done(out) => out.take().expect("batch slot output already taken"),
                _ => unreachable!("batch completed with a non-Done slot"),
            })
            .collect()
    }
}

impl<F: Future> Future for Batch<F> {
    type Output = Vec<F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: structural pin — `slots` and their sub-futures stay pinned in place.
        let me = unsafe { self.get_unchecked_mut() };
        if me.remaining == 0 {
            return Poll::Ready(Self::collect(&mut me.slots));
        }
        loop {
            let admitted = me.admit();
            let mut any_ready = false;
            for i in 0..me.slots.len() {
                let SubSlot::Live(f) = &mut me.slots[i] else {
                    continue;
                };
                // SAFETY: `f` lives in pinned `me.slots`; re-pinned in place, never moved.
                let pinned = unsafe { Pin::new_unchecked(f) };
                if let Poll::Ready(out) = pinned.poll(cx) {
                    me.slots[i] = SubSlot::Done(Some(out));
                    me.in_flight -= 1;
                    me.remaining -= 1;
                    any_ready = true;
                }
            }
            if me.remaining == 0 {
                return Poll::Ready(Self::collect(&mut me.slots));
            }
            if !admitted && !any_ready {
                return Poll::Pending;
            }
        }
    }
}
