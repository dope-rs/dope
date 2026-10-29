use crate::{
    process::{self, gate, runtime::bus::Mode},
    shutdown,
};

#[derive(Clone)]
pub(in crate::process::runtime) struct Plain<E>(E);

#[derive(Clone)]
pub(in crate::process::runtime) struct Gated<E>(E);

pub(in crate::process::runtime) trait Entry<I, X>:
    Mode + Clone + Send
{
    fn run<'run>(
        self,
        input: I,
        context: process::Context<'run>,
    ) -> Result<shutdown::Requested<process::Shutdown<'run>>, X>;
}

impl<E> Plain<E> {
    pub(in crate::process::runtime) const fn new(entry: E) -> Self {
        Self(entry)
    }
}

impl<E> Gated<E> {
    pub(in crate::process::runtime) const fn new(entry: E) -> Self {
        Self(entry)
    }
}

impl<I, E, X> Entry<I, X> for Plain<E>
where
    E: for<'run> Fn(
            I,
            process::Context<'run>,
        ) -> Result<shutdown::Requested<process::Shutdown<'run>>, X>
        + Clone
        + Send,
{
    fn run<'run>(
        self,
        input: I,
        context: process::Context<'run>,
    ) -> Result<shutdown::Requested<process::Shutdown<'run>>, X> {
        (self.0)(input, context)
    }
}

impl<I, E, X> Entry<I, X> for Gated<E>
where
    E: for<'run> Fn(
            I,
            process::Context<'run>,
            gate::Closed<'run>,
        ) -> Result<shutdown::Requested<process::Shutdown<'run>>, X>
        + Clone
        + Send,
{
    fn run<'run>(
        self,
        input: I,
        context: process::Context<'run>,
    ) -> Result<shutdown::Requested<process::Shutdown<'run>>, X> {
        let (context, gate) = context.gated();
        (self.0)(input, context, gate)
    }
}

impl<E> Mode for Plain<E> {
    const GATED: bool = false;
}

impl<E> Mode for Gated<E> {
    const GATED: bool = true;
}
