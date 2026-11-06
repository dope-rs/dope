use dope_core::{
    driver::{receive::Exhausted, route::Identity},
    io::event::net::recv,
};
use receive::eof;

use crate::{
    link::slot::types,
    wire::{self, receive, terminal},
};

pub(in crate::link) enum Retirement<'d> {
    Ready,
    Failed(dope_core::Errno),
    Exhausted(Exhausted<'d>),
    Starved(recv::Retryable),
}

pub(in crate::link) trait Capture<W: wire::Spec> {
    fn capture<'w, 'd, ID: Identity + 'w>(conn: &mut types::Control<'w, 'd, ID, W>)
    where
        W: 'w,
        'd: 'w;
}

impl<W: wire::Spec> Capture<W> for eof::Clean {
    fn capture<'w, 'd, ID: Identity + 'w>(conn: &mut types::Control<'w, 'd, ID, W>)
    where
        W: 'w,
        'd: 'w,
    {
        conn.engine.operations.recv.pause();
    }
}

impl<W> Capture<W> for eof::State
where
    W: wire::Spec,
    W::Receive: eof::Eof<W>,
{
    fn capture<'w, 'd, ID: Identity + 'w>(conn: &mut types::Control<'w, 'd, ID, W>)
    where
        W: 'w,
        'd: 'w,
    {
        if conn.is_recv_done() {
            return;
        }
        let end = match conn.engine.send(&mut conn.terminal) {
            terminal::Admission::Open(end) => end,
            terminal::Admission::Closed => return,
        };
        let done = <W::Receive as eof::Eof<W>>::eof::<ID>(&mut conn.wire, end);
        let ((), captured) = done.into_parts();
        conn.engine.captured(captured);
    }
}
