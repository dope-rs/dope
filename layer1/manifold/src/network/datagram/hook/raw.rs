use std::pin;

use dope_core::{
    driver::{result, retained, route::Identity, schedule},
    io::datagram::ReceiveError,
};

use crate::network::datagram::{
    self,
    hook::{self, Disabled, Enabled, Endpoint, Transit, app, ingress},
    send,
    socket::priority,
};

pub trait Drive<'d, ID: Identity, H, R>
where
    H: datagram::Ingress<'d, ID>,
{
    /// Exact number of coordinator probes needed to observe one idle sweep.
    /// The count remains fixed from installation through finish.
    const COORDS: usize;

    /// # Safety
    /// Retain this exact installed endpoint and all submitted storage at stable
    /// addresses through terminal completion or driver quiescence.
    unsafe fn pre_park<'a, 'turn>(
        endpoint: pin::Pin<&'a mut Endpoint<'d, ID, H>>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> pin::Pin<&'a mut Endpoint<'d, ID, H>>;
}

trait Transfer<'d, ID: Identity, H> {
    fn transferred(handler: pin::Pin<&mut H>, transfer: send::Transfer<'d, ID>);
}

impl<'d, ID: Identity, H> Transfer<'d, ID, H> for Disabled {
    fn transferred(_: pin::Pin<&mut H>, _: send::Transfer<'d, ID>) {}
}

impl<'d, ID: Identity, H: datagram::Transfer<'d, ID>> Transfer<'d, ID, H> for Enabled {
    fn transferred(handler: pin::Pin<&mut H>, transfer: send::Transfer<'d, ID>) {
        handler.transferred(transfer);
    }
}

impl<'d, ID: Identity, H> Drive<'d, ID, H, Disabled> for Disabled
where
    H: datagram::Ingress<'d, ID>,
{
    const COORDS: usize = 0;

    unsafe fn pre_park<'a, 'turn>(
        mut endpoint: pin::Pin<&'a mut Endpoint<'d, ID, H>>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> pin::Pin<&'a mut Endpoint<'d, ID, H>> {
        let this = endpoint.as_mut().project();
        let mut socket = this.socket;
        if !socket.as_ref().get_ref().sender.is_accepting() {
            socket
                .as_mut()
                .project()
                .sender
                .drain(turn.reborrow().maint(), drop);
        }
        let result = priority::run(
            socket,
            this.handler,
            turn.app(),
            driver,
            |_, _, _, _| {},
            |_, _| {},
        );
        submitted(endpoint, result, driver)
    }
}

impl<'d, ID: Identity, H, T> hook::raw::Drive<'d, ID, H, hook::Disabled> for app::Schedule<T>
where
    H: datagram::Ingress<'d, ID> + datagram::Application<'d, ID>,
    T: Transfer<'d, ID, H>,
{
    const COORDS: usize = 0;

    unsafe fn pre_park<'a, 'turn>(
        mut endpoint: pin::Pin<&'a mut Endpoint<'d, ID, H>>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> pin::Pin<&'a mut Endpoint<'d, ID, H>> {
        let this = endpoint.as_mut().project();
        let mut socket = this.socket;
        let now = driver.driver_ref().clock().now();
        if !socket.as_ref().get_ref().sender.is_accepting() {
            socket
                .as_mut()
                .project()
                .sender
                .drain(turn.reborrow().maint(), drop);
        }
        let result = priority::run(
            socket,
            this.handler,
            turn.app(),
            driver,
            |handler, socket, work, driver| handler.pre_park(socket, now, work, driver),
            T::transferred,
        );
        submitted(endpoint, result, driver)
    }
}

impl<'d, ID: Identity, H, T> hook::raw::Drive<'d, ID, H, hook::Enabled> for app::Schedule<T>
where
    H: datagram::Settlement<'d, ID> + datagram::Application<'d, ID>,
    T: Transfer<'d, ID, H>,
{
    const COORDS: usize = 0;

    unsafe fn pre_park<'a, 'turn>(
        mut endpoint: pin::Pin<&'a mut Endpoint<'d, ID, H>>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> pin::Pin<&'a mut Endpoint<'d, ID, H>> {
        let mut this = endpoint.as_mut().project();
        let mut socket = this.socket;
        let now = driver.driver_ref().clock().now();
        if !socket.as_ref().get_ref().sender.is_accepting() {
            socket.as_mut().project().sender.drain_event(
                driver.state_token(),
                turn.reborrow().maint(),
                now,
                this.handler.as_mut(),
            );
        }
        let result = priority::run(
            socket,
            this.handler,
            turn.app(),
            driver,
            |handler, socket, work, driver| handler.pre_park(socket, now, work, driver),
            T::transferred,
        );
        submitted(endpoint, result, driver)
    }
}

impl<'d, ID: Identity, H, T> hook::raw::Drive<'d, ID, H, Enabled> for Transit<T>
where
    H: datagram::Settlement<'d, ID> + datagram::Transport<'d, ID>,
    T: Transfer<'d, ID, H>,
{
    const COORDS: usize = 0;

    unsafe fn pre_park<'a, 'turn>(
        mut endpoint: pin::Pin<&'a mut Endpoint<'d, ID, H>>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> pin::Pin<&'a mut Endpoint<'d, ID, H>> {
        let mut this = endpoint.as_mut().project();
        let mut socket = this.socket;
        let now = driver.driver_ref().clock().now();
        if !socket.as_ref().get_ref().sender.is_accepting() {
            socket.as_mut().project().sender.drain_event(
                driver.state_token(),
                turn.reborrow().maint(),
                now,
                this.handler.as_mut(),
            );
        }
        let result = priority::run(
            socket,
            this.handler,
            turn.app(),
            driver,
            |handler, socket, work, driver| {
                handler.pre_park(socket, now, work, driver.state_token());
            },
            T::transferred,
        );
        submitted(endpoint, result, driver)
    }
}

fn submitted<'a, 'd, ID: Identity, H>(
    mut endpoint: pin::Pin<&'a mut Endpoint<'d, ID, H>>,
    result: Result<(), result::SubmitError>,
    driver: &mut retained::Context<'_, '_, 'd>,
) -> pin::Pin<&'a mut Endpoint<'d, ID, H>>
where
    H: datagram::Ingress<'d, ID>,
{
    if let Err(error) = result {
        <H::Mode as ingress::Failure<'d, ID, H>>::failed(
            endpoint.as_mut(),
            ReceiveError::Submit(error),
            driver.driver_ref().clock().now(),
            driver.state_token(),
        );
    }
    endpoint
}
