use std::{mem, net, ops, pin};

use dope_core::{
    driver::route::{Identity, KeyTag},
    io::{self, datagram::Residency},
};
use dope_runtime::executor;
use o3::buffer::{self, pool};

use crate::{
    app::dispatch::{self, event},
    network::datagram::{
        self,
        hook::{self, ingress},
        send,
    },
};

#[pin_project::pin_project]
pub struct Endpoint<'d, ID: Identity, H, B = pool::Cursor, M: send::Mode = send::Plain> {
    #[pin]
    pub(in crate::network::datagram) socket: super::Socket<'d, ID, B, M>,
    #[pin]
    pub(in crate::network::datagram) handler: H,
}

impl<'d, ID: Identity, H, B: buffer::Residency + buffer::raw::Source, M: send::Mode>
    executor::raw::Finishes<'d, Self> for Endpoint<'d, ID, H, B, M>
where
    H: datagram::Ingress<'d, ID>,
    H::Hooks: hook::Bundle<Buffer = B, Send = M>,
    <H::Hooks as hook::Bundle>::Finalize:
        hook::Finalize<'d, ID, H, <H::Hooks as hook::Bundle>::Settlement>,
{
    fn call<'app, 'finish, 'owner, 'epoch>(
        finish: executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self>,
    ) -> executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self> {
        let (owner, install, proof) = finish.into_parts();
        let owner = <<H::Hooks as hook::Bundle>::Finalize as hook::Finalize<
            'd,
            ID,
            H,
            <H::Hooks as hook::Bundle>::Settlement,
        >>::finalize(owner, proof);
        executor::raw::Finish::new(owner, install, proof)
    }
}

impl<'d, ID: Identity, H, B: buffer::Residency + buffer::raw::Source, M: send::Mode>
    event::Complete<'d> for Endpoint<'d, ID, H, B, M>
where
    H: datagram::Ingress<'d, ID, Mode = ingress::Access> + datagram::Access<'d, ID>,
    H::Hooks: hook::Bundle<Buffer = B, Send = M, Settlement = hook::Disabled>,
{
    fn dispatch<'app>(
        call: executor::raw::Call<'app, Self, (), io::Event<'d>>,
    ) -> ops::ControlFlow<io::Event<'d>> {
        let (mut endpoint, (), event) = call.into_parts();
        let this = endpoint.as_mut().project();
        let mut socket = this.socket;
        let event = match socket.as_mut().project().receive.route(event) {
            ops::ControlFlow::Continue(packet) => {
                packet.dispatch(|packet| {
                    this.handler.packet(packet, socket.as_mut());
                });
                return ops::ControlFlow::Continue(());
            }
            ops::ControlFlow::Break(event) => event,
        };
        socket.release(event)
    }
}

impl<'d, ID: Identity, H, B: buffer::Residency + buffer::raw::Source, M: send::Mode>
    event::Drive<'d> for Endpoint<'d, ID, H, B, M>
where
    H: datagram::Ingress<'d, ID>,
    H::Hooks: hook::Bundle<Buffer = B, Send = M>,
    H::Mode: ingress::Drive<'d, ID, H, <H::Hooks as hook::Bundle>::Settlement>,
{
    type Access = dispatch::raw::Plain;

    fn dispatch<'app, 'driver, 'root>(
        call: executor::raw::Call<
            'app,
            Self,
            (),
            (
                io::Event<'d>,
                dispatch::raw::Context<'driver, 'root, 'd, Self::Access>,
            ),
        >,
    ) -> ops::ControlFlow<io::Event<'d>>
    where
        'd: 'root,
    {
        let (mut endpoint, (), (event, mut driver)) = call.into_parts();
        let clock = driver.driver_ref().clock();
        let this = endpoint.as_mut().project();
        <H::Mode as ingress::Drive<'d, ID, H, <H::Hooks as hook::Bundle>::Settlement>>::dispatch(
            this.socket,
            event,
            this.handler,
            clock,
            driver.state_token(),
        )
    }
}

impl<'d, ID: Identity, H, B, M: send::Mode> Endpoint<'d, ID, H, B, M> {
    pub fn handler(&self) -> &H {
        &self.handler
    }

    pub fn handler_mut(self: pin::Pin<&mut Self>) -> pin::Pin<&mut H> {
        self.project().handler
    }

    /// Borrows the pinned handler and socket exclusively for the same endpoint borrow.
    pub fn split(
        self: pin::Pin<&mut Self>,
    ) -> (pin::Pin<&mut H>, pin::Pin<&mut super::Socket<'d, ID, B, M>>) {
        let this = self.project();
        (this.handler, this.socket)
    }

    pub fn local_addr(&self) -> net::SocketAddr {
        self.socket.binding.local_addr()
    }

    pub fn residency(&self) -> Option<Residency> {
        self.socket.receive.residency()
    }
}

const _: () = {
    assert!(
        mem::size_of::<Endpoint<'static, KeyTag<0>, (), pool::Cursor>>()
            == mem::size_of::<super::Socket<'static, KeyTag<0>, pool::Cursor>>()
    );
    assert!(
        mem::size_of::<Endpoint<'static, KeyTag<0>, (), Vec<u8>>>()
            == mem::size_of::<super::Socket<'static, KeyTag<0>, Vec<u8>>>()
    );
};

mod raw;
