use data::compose;
use dope_core::driver::route::Identity;
use dope_net::link::egress::{self, data};
use o3::base::cell::region;

use crate::client::{
    conn,
    port::{self, error},
};

pub trait Branches<'d, L, R, ID: Identity> {
    fn try_left(
        &self,
        region: &mut region::Token<region::Identity<'d>>,
        conn: conn::Id<'d, ID>,
        load: egress::NonEmpty<L>,
    ) -> Result<(), error::Unsent<'d, L, ID>>;

    fn try_right(
        &self,
        region: &mut region::Token<region::Identity<'d>>,
        conn: conn::Id<'d, ID>,
        load: egress::NonEmpty<R>,
    ) -> Result<(), error::Unsent<'d, R, ID>>;
}

impl<'e, 'd, L: data::raw::Payload<'d>, R: data::raw::Payload<'d>, ID: Identity>
    Branches<'d, L, R, ID> for port::Port<'e, 'd, compose::Either<L, R>, ID>
{
    fn try_left(
        &self,
        region: &mut region::Token<region::Identity<'d>>,
        conn: conn::Id<'d, ID>,
        load: egress::NonEmpty<L>,
    ) -> Result<(), error::Unsent<'d, L, ID>> {
        self.core
            .try_project(region, conn, load, |queue, region, load| {
                queue.try_left(region, load)
            })
    }

    fn try_right(
        &self,
        region: &mut region::Token<region::Identity<'d>>,
        conn: conn::Id<'d, ID>,
        load: egress::NonEmpty<R>,
    ) -> Result<(), error::Unsent<'d, R, ID>> {
        self.core
            .try_project(region, conn, load, |queue, region, load| {
                queue.try_right(region, load)
            })
    }
}
