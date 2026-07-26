use core::pin::Pin;
use core::task::Poll;
use std::process::abort;

use pin_project::pin_project;

use super::Fiber;
use crate::raw::batch::{BatchCore, BatchOutput, PollStep};
use crate::raw::task::Context;

const BATCH_POLL_BUDGET: usize = 32;

#[derive(Clone, Copy, PartialEq, Eq)]
enum BatchStatus {
    Idle,
    Polling,
    Completed,
    Poisoned,
}

struct BatchTransaction<'a> {
    status: &'a mut BatchStatus,
    active: bool,
}

impl BatchTransaction<'_> {
    fn finish(mut self, status: BatchStatus) {
        *self.status = status;
        self.active = false;
    }
}

impl Drop for BatchTransaction<'_> {
    fn drop(&mut self) {
        if self.active {
            *self.status = BatchStatus::Poisoned;
        }
    }
}

#[pin_project]
pub struct Batch<F, O, const N: usize> {
    #[pin]
    core: BatchCore<F, O, N>,
    remaining: usize,
    status: BatchStatus,
}

impl<F, O, const N: usize> Batch<F, O, N> {
    pub fn empty() -> Self {
        Self {
            core: BatchCore::new(),
            remaining: 0,
            status: BatchStatus::Idle,
        }
    }

    pub fn from_array(fibers: [F; N]) -> Self {
        Self {
            core: BatchCore::from_array(fibers),
            remaining: N,
            status: BatchStatus::Idle,
        }
    }

    pub fn try_push(&mut self, fiber: F) -> Result<(), F> {
        if self.status != BatchStatus::Idle {
            return Err(fiber);
        }
        self.core.try_push(fiber)?;
        self.remaining += 1;
        Ok(())
    }
}

impl<'d, F, O, const N: usize> Fiber<'d> for Batch<F, O, N>
where
    F: Fiber<'d, Output = O>,
{
    type Output = BatchOutput<O, N>;

    fn poll(self: Pin<&mut Self>, mut context: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let mut this = self.project();
        if *this.status != BatchStatus::Idle {
            abort();
        }
        *this.status = BatchStatus::Polling;
        let transaction = BatchTransaction {
            status: this.status,
            active: true,
        };

        if *this.remaining == 0 {
            transaction.finish(BatchStatus::Completed);
            return Poll::Ready(this.core.take_output());
        }

        for _ in 0..BATCH_POLL_BUDGET {
            match this.core.as_mut().poll_one(context.as_mut()) {
                PollStep::Idle => {
                    transaction.finish(BatchStatus::Idle);
                    return Poll::Pending;
                }
                PollStep::Pending => {}
                PollStep::Ready => {
                    *this.remaining -= 1;
                    if *this.remaining == 0 {
                        transaction.finish(BatchStatus::Completed);
                        return Poll::Ready(this.core.take_output());
                    }
                }
            }
        }

        let has_work = this.core.as_ref().has_work();
        transaction.finish(BatchStatus::Idle);
        if has_work {
            context.wake();
        }
        Poll::Pending
    }
}
