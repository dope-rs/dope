use std::pin;

use dope_core::{
    driver::{retained, route::Identity, schedule},
    io::event::net::send::Completion,
};
use handler::{Runtime, send};

use crate::{
    app::env,
    network::listener::{
        self, conn, handler,
        runtime::control,
        writer::{self, phase},
    },
};

pub(in crate::network::listener) fn pump_send<'w, 'd: 'w, ID: Identity, A, E>(
    mut listener: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
    done: Completion<'d>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
) where
    A: handler::Application<'d, ID> + Runtime<'w, 'd, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    use dope_net::link::event::{SendFailure, SendOutcome};
    use writer::state::Settled;

    enum Direct<B> {
        More,
        Done(Option<B>),
        Close,
    }

    let mut this = listener.as_mut().project();
    let mut closed = None;
    let result = this.owner.settle(
        done,
        driver,
        |direct| {
            use dope_net::link::event::DirectStatus;

            let (state, status) = direct.into_parts();
            match status {
                DirectStatus::Rejected(error) => {
                    closed = state
                        .send
                        .fail(conn::Transmit::Rejected(SendFailure::Direct(error)));
                    Direct::Close
                }
                DirectStatus::Failed(error) => {
                    closed = state
                        .send
                        .fail(conn::Transmit::Io(SendFailure::Direct(error)));
                    Direct::Close
                }
                DirectStatus::Closed => {
                    closed = state.send.release();
                    Direct::Close
                }
                DirectStatus::Sent(sent) => match state.send.settle(sent) {
                    Settled::More => Direct::More,
                    Settled::Done(source) => Direct::Done(source),
                    Settled::Invalid => Direct::Close,
                },
            }
        },
        |key, body| {
            writer::release_payload(this.app.as_mut(), key, body, handler::Disposition::Sent);
        },
    );
    let key = match &result {
        SendOutcome::Queued { key, .. }
        | SendOutcome::Direct { key, .. }
        | SendOutcome::WriteShutdown { key }
        | SendOutcome::Terminal { key }
        | SendOutcome::Rejected { key, .. }
        | SendOutcome::Failed { key, .. }
        | SendOutcome::Invalid { key, .. } => *key,
        SendOutcome::Close(key) => *key,
        SendOutcome::CloseSent { key, .. } => *key,
        SendOutcome::Drop => return,
    };
    if let Some(egress) = closed {
        writer::release_egress(this.app.as_mut(), key, egress, handler::Disposition::Closed);
    }
    let failure = match result {
        SendOutcome::Queued { .. } | SendOutcome::WriteShutdown { .. } => {
            return A::send(send::Pending::new(
                listener.as_mut(),
                key,
                send::Next::Commit,
                turn,
                driver,
            ));
        }
        SendOutcome::Terminal { .. } => {
            return phase::commit_end(listener.as_mut(), key, turn, driver);
        }
        SendOutcome::Rejected { failure, .. } => Some(conn::Transmit::Rejected(failure)),
        SendOutcome::Failed { failure, .. } => Some(conn::Transmit::Io(failure)),
        SendOutcome::Direct {
            settled: Direct::More,
            ..
        } => return phase::commit_chunk(listener.as_mut(), key, phase::Wake::Send, turn, driver),
        SendOutcome::Direct {
            settled: Direct::Done(source),
            ..
        } => {
            if let Some(egress) = source {
                writer::release_egress(this.app.as_mut(), key, egress, handler::Disposition::Sent);
            }
            return A::send(send::Pending::new(
                listener.as_mut(),
                key,
                send::Next::Done,
                turn,
                driver,
            ));
        }
        SendOutcome::Invalid { error, .. } => Some(conn::Transmit::Invalid(error)),
        SendOutcome::Close(_)
        | SendOutcome::CloseSent { .. }
        | SendOutcome::Direct {
            settled: Direct::Close,
            ..
        } => None,
        SendOutcome::Drop => return,
    };
    if let Some(failure) = failure {
        let closed = this
            .owner
            .pool_mut()
            .get_mut(key)
            .and_then(|slot| slot.state.send.fail(failure));
        if let Some(egress) = closed {
            writer::release_egress(this.app.as_mut(), key, egress, handler::Disposition::Closed);
        }
    }
    control::close_slot(listener, key, turn, driver);
}
