use dope_core::driver::route::Identity;
use dope_net::link::egress::data;

use crate::{
    app::receive::Failure,
    client::{app, close, session::turns},
};

pub(in crate::client::session::app) enum Run {
    Complete,
    Yield,
    Blocked,
    Close,
    Failed(Failure),
}

impl Run {
    pub(in crate::client::session::app) fn finish<
        'e,
        'd,
        C,
        B: data::raw::Payload<'d>,
        ID: Identity,
    >(
        self,
        ctx: &mut turns::Context<'_, 'e, 'd, C, B, ID>,
    ) -> app::Resumed {
        use app::ChunkOutcome;

        let next = match self {
            Self::Close => {
                ctx.request(close::Directive::reconnect(close::Reason::Protocol));
                ChunkOutcome::ok()
            }
            Self::Yield => return app::Resumed::Paused(app::Pause::Yield),
            Self::Blocked => return app::Resumed::Paused(app::Pause::Blocked),
            Self::Failed(cause) => ChunkOutcome::failed(cause),
            Self::Complete => ChunkOutcome::ok(),
        };
        app::Resumed::Complete(next)
    }
}
