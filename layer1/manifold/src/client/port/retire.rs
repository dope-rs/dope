use dope_core::driver::{route::Identity, schedule::app};
use dope_net::link::egress::{
    self, data,
    metadata::{self, arena},
};
use o3::base::cell::region;

use crate::{
    app::dispatch::identity::Identity as _,
    client::{
        conn,
        port::{Port, state},
    },
};

impl<'e, 'd, B: data::raw::Payload<'d>, ID: Identity> Port<'e, 'd, B, ID> {
    pub fn retire<'turn>(
        &self,
        conn: conn::Id<'d, ID>,
        work: app::Work<'turn, 'd>,
        region: &mut region::Token<region::Identity<'d>>,
        cancel: impl FnOnce(B),
    ) -> egress::Cleared {
        let Some(slot) = self.arena.get(metadata::Lane::from_slot(conn.slot())) else {
            return egress::Cleared::Done;
        };
        let drain = match slot.state().begin_retire(conn) {
            Ok(Some(drain)) => drain,
            Ok(None) => return egress::Cleared::Done,
            Err(state::Busy) => return egress::Cleared::Retry,
        };
        match slot.clear_step(region, work, cancel) {
            arena::Progress::Done => {
                drain.finish();
                egress::Cleared::Done
            }
            arena::Progress::Retry => egress::Cleared::Retry,
        }
    }
}
