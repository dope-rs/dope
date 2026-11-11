use dope::core::driver::settings::claim;
use dope::core::driver::settings::errors::Claim;
use dope::core::driver::{self, settings};
use o3::buffer::resident;

use crate::conn::id;
use crate::conn::life::{accept, options};
use crate::errors;
use crate::runtime::embedded::{self, plan, server};
use crate::runtime::mux::{app, auth, builder, handler, router};
use crate::runtime::routing;
use crate::stream;

/// Validated, single-use socket-free server auth.
#[must_use = "an embedded server plan must be bound to one driver"]
pub struct Plan<const ID: u8> {
    auth: accept::Authority<ID>,
    proto: builder::Plan,
}

impl<const ID: u8> Plan<ID> {
    pub fn new(
        source: rustls::ServerConfig,
        opts: options::Options,
        plan: plan::Plan,
    ) -> Result<Self, errors::Connect> {
        let auth = accept::Authority::new(source, opts)?;
        Ok(Self {
            auth,
            proto: plan.limits(),
        })
    }

    pub fn bind<'d, H, B>(
        self,
        handler: H,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<embedded::Endpoint<'d, ID, H, B>, server::BindError>
    where
        H: handler::Handler<ID, B, resident::Shared<'d>, id::Driver<'d>>,
        B: stream::Recv,
    {
        let mux = self.build(handler, driver)?;
        let ready = embedded::try_ready(driver.driver_ref()).map_err(server::BindError::Ready)?;
        Ok(embedded::Endpoint::new(mux, embedded::Idle, ready))
    }

    pub fn bind_with<'d, H, B, A, F, E>(
        self,
        driver: &mut driver::Context<'_, 'd>,
        bind: F,
    ) -> Result<embedded::Endpoint<'d, ID, H, B, embedded::Run<A>>, server::BindFailure<E>>
    where
        H: handler::Handler<ID, B, resident::Shared<'d>, id::Driver<'d>>,
        B: stream::Recv,
        A: app::Driver<'d, H, ID, B, resident::Shared<'d>>,
        F: FnOnce(driver::Binding<'d>) -> Result<(H, A), E>,
    {
        let binding = driver::Binding::new(driver.driver_ref().timer(), self.proto.owners());
        let (handler, app) = bind(binding).map_err(server::BindFailure::Application)?;
        let mux = self
            .build(handler, driver)
            .map_err(server::BindFailure::Endpoint)?;
        let schedule = embedded::Run {
            app,
            work: embedded::WorkGraph::new(),
        };
        let ready = embedded::try_ready(driver.driver_ref())
            .map_err(server::BindError::Ready)
            .map_err(server::BindFailure::Endpoint)?;
        Ok(embedded::Endpoint::new(mux, schedule, ready))
    }

    fn build<'d, H, B>(
        self,
        handler: H,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<
        router::Router<H, ID, B, auth::Owned<routing::Plain, id::Driver<'d>>>,
        server::BindError,
    >
    where
        H: handler::Handler<ID, B, resident::Shared<'d>, id::Driver<'d>>,
        B: stream::Recv,
    {
        let Self { auth, proto: plan } = self;
        builder::Builder::<auth::Owned<routing::Plain, id::Driver<'d>>, _, ID, B>::server_in(
            handler,
            auth,
            routing::Plain,
            plan,
        )
        .bind(driver)
        .map_err(server::BindError::Mux)
    }
}

impl<const ID: u8> settings::Resource for Plan<ID> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, Claim> {
        needs.include(plan::claim::<ID>(self.proto))
    }
}
