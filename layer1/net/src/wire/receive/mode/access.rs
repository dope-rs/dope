use std::ops::ControlFlow;

use o3::{
    buffer,
    buffer::{hold, write::consume},
};

/// Projects the current contiguous head while retaining the complete input.
/// `Source::remaining` counts all heads; views and consumption cover only the
/// current head. Shared inspection never advances to another head.
pub trait Cursor<'d, A>:
    buffer::Complete + hold::raw::Retain<'d> + consume::Consume + buffer::raw::Consume + Sized
{
    fn drive<T>(&mut self, done: T, use_head: &mut impl Use<'d, T>) -> T;
}

/// One borrowed cursor head whose consuming transition commits its progress.
pub trait Head<'d>:
    buffer::Complete + hold::raw::Retain<'d> + consume::Consume + buffer::raw::Consume
{
    fn commit(self);
}

/// An infallible segmented cursor whose current head stays tied to its owner.
/// A nonempty source exposes a nonempty head. Committing its complete prefix
/// selects the next head without releasing the cursor's backing owners.
pub trait Sequence<'d>:
    buffer::Complete + hold::raw::Retain<'d> + consume::Consume + buffer::raw::Consume
{
    type Head<'head>: Head<'d>
    where
        Self: 'head;

    fn next(&mut self) -> Option<Self::Head<'_>>;
}

pub trait Use<'d, T> {
    /// Consumes input before returning `Continue` to request further traversal.
    /// Return `Break` when processing cannot advance: segmented traversal commits
    /// the consumed prefix and may offer the same head's remaining bytes again.
    fn apply<R>(&mut self, head: &mut R) -> ControlFlow<T, T>
    where
        R: buffer::Complete + hold::raw::Retain<'d> + consume::Consume + buffer::raw::Consume;
}

/// Consumes each actual cursor head before its storage is released.
pub struct Discard;

impl<'d> Use<'d, ()> for Discard {
    fn apply<R>(&mut self, head: &mut R) -> ControlFlow<()>
    where
        R: buffer::Complete + hold::raw::Retain<'d> + consume::Consume + buffer::raw::Consume,
    {
        head.consume_max(usize::MAX);
        ControlFlow::Continue(())
    }
}

/// Executes either one direct head or every requested head in a sequence.
pub trait Drive<'d, R: buffer::Source>: Sized {
    fn drive<T>(cursor: &mut R, done: T, use_head: &mut impl Use<'d, T>) -> T;
}

pub enum Direct {}

pub enum Segments {}

impl<'d, R: buffer::Complete + hold::raw::Retain<'d> + consume::Consume + buffer::raw::Consume>
    Drive<'d, R> for Direct
{
    fn drive<T>(cursor: &mut R, done: T, use_head: &mut impl Use<'d, T>) -> T {
        if cursor.is_empty() {
            return done;
        }
        match use_head.apply(cursor) {
            ControlFlow::Continue(done) | ControlFlow::Break(done) => done,
        }
    }
}

impl<'d, R: buffer::Complete + hold::raw::Retain<'d> + consume::Consume + buffer::raw::Consume>
    Cursor<'d, Direct> for R
{
    fn drive<T>(&mut self, done: T, use_head: &mut impl Use<'d, T>) -> T {
        Direct::drive(self, done, use_head)
    }
}

impl<'d, R: Sequence<'d>> Drive<'d, R> for Segments {
    fn drive<T>(cursor: &mut R, mut done: T, use_head: &mut impl Use<'d, T>) -> T {
        loop {
            let Some(mut head) = cursor.next() else {
                return done;
            };
            let flow = use_head.apply(&mut head);
            head.commit();
            match flow {
                ControlFlow::Continue(next) => done = next,
                ControlFlow::Break(done) => return done,
            }
        }
    }
}

impl<'d, R: Sequence<'d>> Cursor<'d, Segments> for R {
    fn drive<T>(&mut self, done: T, use_head: &mut impl Use<'d, T>) -> T {
        Segments::drive(self, done, use_head)
    }
}
