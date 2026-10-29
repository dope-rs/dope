use dope_core::driver::storage;

use crate::{
    executor::{self, session},
    shutdown,
    topology::{self, Worker, lending, recipe},
};

/// Runs once in one exact driver generation, retaining its operation owner.
pub trait Operation<'d, C>: Sized {
    type Output;

    fn run(self, context: C) -> Self::Output
    where
        Self: 'd;
}

impl<'d, C, F, R> Operation<'d, C> for F
where
    F: FnOnce(C) -> R,
{
    type Output = R;

    fn run(self, context: C) -> Self::Output
    where
        Self: 'd,
    {
        self(context)
    }
}

impl<S, I, Q, O, R> executor::Op<S, Q> for (I, O)
where
    S: storage::Plan,
    O: for<'scope, 'life, 'p, 'd> Operation<'d, Worker<'scope, 'life, 'p, 'd, S, I, Q>, Output = R>,
{
    type Output = R;

    fn run<'scope, 'life, 'p, 'd>(
        self,
        session: session::Scope<'scope, 'life, 'p, 'd, S, Q>,
    ) -> Self::Output
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        'life: 'scope,
        Self: 'd,
        S: 'd,
        Q: 'd,
    {
        self.1.run(Worker::new(session, self.0))
    }
}

/// Maps an operation's output before its driver generation is released.
pub struct Map<O, F>(pub O, pub F);

impl<'d, C, O, F, R> Operation<'d, C> for Map<O, F>
where
    O: Operation<'d, C>,
    F: FnOnce(O::Output) -> R,
{
    type Output = R;

    fn run(self, context: C) -> R
    where
        Self: 'd,
    {
        (self.1)(self.0.run(context))
    }
}

/// Installs the claimed head and lends it to its retained dependent tail.
pub struct Compose;

impl<'scope, 'life, 'p, 'd, S, I, T, Q> Operation<'d, Worker<'scope, 'life, 'p, 'd, S, (I, T), Q>>
    for Compose
where
    S: storage::Plan,
    Q: executor::Startup,
    I: lending::ComposeFrom<S, T, Q>,
{
    type Output = Result<I::Output, I::Error>;

    fn run(self, worker: Worker<'scope, 'life, 'p, 'd, S, (I, T), Q>) -> Self::Output
    where
        Self: 'd,
    {
        worker.lend(|(head, tail), stage| head.compose(tail, stage))
    }
}

/// Installs and runs the retained application, preserving its shutdown proof.
pub struct Run<'report>(pub &'report mut session::Completion);

impl<'report, 'scope, 'life, 'p, 'd, S, I, Q> Operation<'d, Worker<'scope, 'life, 'p, 'd, S, I, Q>>
    for Run<'report>
where
    S: storage::Plan,
    Q: executor::Startup,
    I: recipe::Execute<S>,
{
    type Output = Result<
        shutdown::Requested<Q>,
        topology::Failure<'report, I::Error, shutdown::Requested<Q>>,
    >;

    fn run(self, worker: Worker<'scope, 'life, 'p, 'd, S, I, Q>) -> Self::Output
    where
        Self: 'd,
    {
        worker.lend(|source, stage| source.execute(stage, self.0))
    }
}
