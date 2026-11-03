use o3::buffer;
use std::{ops, pin, time};

use crate::runtime::mux::handler::hook::{
    data::{datagram, release::Mode as _, stream},
    life::{close::Mode as _, closing::Mode as _, early, ready},
    set::{Datagram as _, Dispatch, Early as _, Established as _, Stream as _},
};
use crate::stream::Recv;
use crate::{
    conn,
    runtime::{mux, routing::registry},
};
use conn::id;
use mux::{auth, handler, router};
use router::drive;

struct Notification<'scope, H, const DOMAIN: u8, B, D, I, T>
where
    H: handler::Handler<DOMAIN, B, D, I>,
    B: Recv,
    D: buffer::raw::Segment,
    I: id::Temporal,
{
    handle: id::Handle<I, DOMAIN>,
    sink: pin::Pin<&'scope mut H>,
    slot: registry::Access<'scope, H::Connection, DOMAIN, B, D, T, false>,
    at: I::Observation,
}

impl<H, const DOMAIN: u8, B, D, I, T> Notification<'_, H, DOMAIN, B, D, I, T>
where
    H: handler::Handler<DOMAIN, B, D, I>,
    B: Recv,
    D: buffer::raw::Segment,
    I: id::Temporal,
{
    fn reject(&mut self) -> ops::ControlFlow<()> {
        <H::Events as Dispatch<H, DOMAIN, B, D, I>>::Early::call(move || {
            match self.slot.conn.events().early() {
                conn::Early::None => return None,
                conn::Early::Rejected => {}
            }
            Some(move || {
                let this = self;
                let Some(app) = this.slot.connection.as_mut() else {
                    unreachable!("application")
                };
                early::Event::new(this.sink.as_mut(), app, this.handle)
            })
        })
    }

    fn establish(&mut self) -> ops::ControlFlow<()> {
        <H::Events as Dispatch<H, DOMAIN, B, D, I>>::Established::call(move || {
            if !self.slot.conn.view().lifecycle().is_ready() || *self.slot.notified_established {
                return None;
            }
            *self.slot.notified_established = true;
            Some(move || {
                let this = self;
                let Some(app) = this.slot.connection.as_mut() else {
                    unreachable!("application")
                };
                ready::Event::new(
                    this.sink.as_mut(),
                    handler::Slot::new(app, this.slot.conn.reborrow::<true>()),
                    this.handle,
                    this.at,
                    this.slot.notified_established,
                )
            })
        })
    }

    fn datagram(&mut self) -> ops::ControlFlow<()> {
        if <H::Events as Dispatch<H, DOMAIN, B, D, I>>::Datagram::PULL {
            if self.slot.conn.events().has_datagram() {
                self.slot.conn.application().ready();
            }
            return ops::ControlFlow::Continue(());
        }
        <H::Events as Dispatch<H, DOMAIN, B, D, I>>::Datagram::call(move || {
            let data = self.slot.conn.datagrams().recv()?;
            *self.slot.stream_first = true;
            Some(move || {
                let Some(app) = self.slot.connection.as_mut() else {
                    unreachable!("application")
                };
                datagram::Event::new(
                    self.sink.as_mut(),
                    handler::Slot::new(app, self.slot.conn.reborrow::<true>()),
                    self.handle,
                    data,
                    self.at,
                )
            })
        })
    }

    /// Early deferrals retain their event without rearming its notification.
    fn stream(&mut self) -> ops::ControlFlow<()> {
        if <H::Events as Dispatch<H, DOMAIN, B, D, I>>::Stream::PULL {
            if self.slot.conn.events().has_stream() {
                self.slot.conn.application().ready();
            }
            return ops::ControlFlow::Continue(());
        }
        let Some(lease) = self.slot.conn.events().poll_stream() else {
            return ops::ControlFlow::Continue(());
        };
        *self.slot.stream_first = false;
        let ready = *self.slot.notified_established;
        let owner = &mut *self;
        let bound = &lease;
        let event = move || {
            let this = owner;
            let Some(app) = this.slot.connection.as_mut() else {
                unreachable!("application")
            };
            stream::Event::new(
                this.sink.as_mut(),
                handler::Slot::new(app, this.slot.conn.reborrow::<true>()),
                this.handle,
                bound.bound_event(),
                this.at,
            )
        };
        let action = if ready {
            <H::Events as Dispatch<H, DOMAIN, B, D, I>>::Stream::call(event)
        } else {
            <H::Events as Dispatch<H, DOMAIN, B, D, I>>::Stream::early(event)
        };
        self.slot
            .conn
            .events()
            .finish(lease, action == mux::Disposition::Accept);
        if ready || action == mux::Disposition::Accept {
            ops::ControlFlow::Break(())
        } else {
            ops::ControlFlow::Continue(())
        }
    }

    fn data(&mut self) -> ops::ControlFlow<()> {
        let first = *self.slot.notified_established && *self.slot.stream_first;
        if !first {
            self.datagram()?;
        }
        self.stream()?;
        if first {
            self.datagram()?;
        }
        ops::ControlFlow::Continue(())
    }

    fn dispatch(&mut self) {
        if self.slot.conn.view().lifecycle().is_closing() {
            let Some(mut app) = self.slot.connection.take() else {
                return;
            };
            <H::Events as Dispatch<H, DOMAIN, B, D, I>>::Closing::notify(
                self.sink.as_mut(),
                handler::Slot::new(&mut app, self.slot.conn.reborrow::<true>()),
                self.handle,
            );
            <H::Events as Dispatch<H, DOMAIN, B, D, I>>::Release::cancel(
                self.sink.as_mut(),
                self.slot.conn.reborrow::<false>(),
                self.handle,
            );
            let brand = self.slot.conn.brand();
            <H::Events as Dispatch<H, DOMAIN, B, D, I>>::Close::close(
                self.sink.as_mut(),
                handler::Close::new(brand.reborrow(), self.at.into()),
                app,
                self.handle,
            );
            return;
        }
        if self.reject().is_break() {
            self.slot.conn.events().mark(conn::Effects::NOTIFY);
            return;
        }
        if self.establish().is_break() {
            if *self.slot.notified_established {
                self.slot.conn.events().mark(conn::Effects::NOTIFY);
            }
            return;
        }
        if self.data().is_break() {
            self.slot.conn.events().mark(conn::Effects::NOTIFY);
        }
    }
}

pub(in crate::runtime::mux) fn notify<
    A: auth::Mode<DOMAIN>,
    H: handler::Handler<DOMAIN, B, A::Send, A::Identity>,
    const DOMAIN: u8,
    B: Recv,
>(
    turn: &mut router::Turn<'_, A, H, DOMAIN, B>,
    at: <A::Identity as id::Temporal>::Observation,
) -> drive::Step {
    let now = Into::<time::Instant>::into(at);
    let Some(()) = turn.ready::<(), { mux::NOTIFY_QUEUE }>(now, |handle, sink, slot| {
        Notification {
            handle,
            sink,
            slot,
            at,
        }
        .dispatch();
    }) else {
        return drive::Step::Idle;
    };
    drive::Step::Driven
}
