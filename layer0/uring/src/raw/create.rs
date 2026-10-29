use dope_sys::{
    engine::ring::Backend as _,
    io::socket::option::tuning::Create as _,
    probe::{Build as _, Context as _, Validate as _, receive::Validate as _},
    ring::create::{Acquire as _, Retain as _},
    table::permit::Create as _,
};

use crate::{
    Admission, Built, Cause, Created, Engine, Host, Primary, Range, Ready, Recovery, Rejection,
    Removal, Removed, Retained, Ring, Session, Update, Uring, Validate, config, engine, host,
    memory, num, permit, probe, ready, receive, session,
};

impl<C, G> engine::raw::Create<C, G> for Uring
where
    C: config::Config<G>,
    C::Layout: memory::raw::Canary,
    Host<C>: probe::Build<Update>,
    Cause<C>: From<Primary> + From<memory::Error> + From<Admission>,
    Ring<C::Token>: probe::Validate<
            C::Layout,
            Cause<C>,
            Probe = <Host<C> as host::Context>::Probe,
            Cleanup = Recovery<C>,
        > + Validate<
            C::Layout,
            Cause<C>,
            Removal<C>,
            Range = Range,
            Probe = <Host<C> as host::Context>::Probe,
            Update = Update,
            Cleanup = Recovery<C>,
            Release = Removed<C>,
        >,
    Session<C::Token, C::Layout>: session::Create<Cause<C>, Ring = Ring<C::Token>, Cleanup = Retained<C>>
        + receive::Validate<
            Cause<C>,
            Removal<C>,
            Update = Update,
            Cleanup = Recovery<C>,
            Release = Removed<C>,
        >,
{
    type Receive = <Self as permit::Source>::Queue<C::Key, C::Index>;
    type Error = Created<C>;

    unsafe fn try_new(config: &G) -> Result<Built<C, G>, Self::Error> {
        let config = C::config(config);
        let receive = Self::Receive::try_new(config.files.get() as usize)
            .map_err(Created::<C>::Allocation)?;
        let count = num::NonZero::<u32>::MIN.saturating_add(config.files.get() - 1);
        let tuning = C::Tuning::try_new(count).map_err(Created::<C>::Allocation)?;
        let (ring, range) = Ring::<C::Token>::acquire::<_, Removed<C>>(
            config.submit,
            config.complete,
            config.defer,
            config.files,
            config.range,
        )?;
        let recover =
            |result: Recovery<C>| Created::<C>::from(result.map_err(Retained::<C>::probe));
        let reject = |error: Rejection<Recovery<C>, Removed<C>>| match error {
            Rejection::Cleanup(result) => recover(result),
            Rejection::Release(result) => Created::<C>::Release(result),
        };
        let ring = ring.support().map_err(recover)?;
        let session = <Session<C::Token, C::Layout> as session::Create<Cause<C>>>::try_new(ring)
            .map_err(Created::<C>::from)?;
        let session = session::Create::socket(session).map_err(Created::<C>::from)?;
        let session = session.receive(Host::<C>::remove).map_err(reject)?;
        let ring = session::Create::finish(session).map_err(Created::<C>::from)?;
        let ring = match range {
            Some(range) => Validate::accept(ring, range, Host::<C>::release).map_err(reject)?,
            None => ring,
        };
        let ring = ring.empty(Host::<C>::RING).map_err(recover)?;
        // SAFETY: the caller retains this exact returned Context beside the
        // backend through quiescence and destruction. Failed registration
        // retains its Context inside the complete native cleanup owner.
        let (ring, context) = unsafe {
            <Ready<C::Token, C::Layout> as ready::raw::Bind<Cause<C>>>::bind(ring, config.receive)
        }
        .map_err(Created::<C>::from)?;
        Ok((Engine::new(ring, tuning), context, receive))
    }
}
