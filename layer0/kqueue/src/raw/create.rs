use crate::{Create, Kqueue, config, engine, permit, reactor, ready};

impl<C, G> engine::raw::Create<C, G> for Kqueue
where
    C: config::Config<G>,
{
    type Receive = <Self as permit::Source>::Queue<C::Key, C::Index>;
    type Error = Create;

    unsafe fn try_new(
        config: &G,
    ) -> Result<
        (
            Self::Backend,
            <Self::Backend as engine::Buffer>::Context,
            Self::Receive,
        ),
        Self::Error,
    > {
        use permit::Empty as _;
        use ready::Backend as _;

        let config = C::config(config);
        let (backend, context) =
            reactor::Engine::try_new(config.receive, config.slots, config.plan, config.capacity)?;
        let receive = Self::Receive::new();
        Ok((backend, context, receive))
    }
}
