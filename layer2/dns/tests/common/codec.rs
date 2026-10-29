use std::pin;

use dope::manifold::client::session::codec::{Codec, Input, staging::Policy};
use dope_dns::Error;
use o3::{
    base::cell::region,
    buffer::{self, resident, resident::staging},
};

pub fn input<'input, 'd, C: Codec, R: buffer::Cursor, T>(
    codec: &C,
    source: &'input R,
    budget: &'input resident::Budget<'d>,
    run: impl for<'id> FnOnce(Input<'id, 'input, 'd, R>) -> T,
) -> Result<T, staging::Error> {
    let plan = <C::Staging as Policy>::plan(codec.staging())?;
    let limit = <C::Staging as Policy>::limit(plan);
    Ok(Input::with(source, budget, limit, run))
}

pub fn try_with_budget<R>(
    capacity: usize,
    run: impl for<'d> FnOnce(resident::Budget<'d>) -> R,
) -> Result<R, resident::Reuse> {
    region::Token::scope_pinned(pin::pin!(resident::Domain::new()), |token, domain| {
        let budget = domain.get_mut().try_budget(capacity, token.proof())?;
        Ok(run(budget))
    })
}

pub const COMPLETE: [u8; 5] = [0, 3, 7, 8, 9];
pub const PAYLOAD: [u8; 3] = [7, 8, 9];
pub const TRUNCATED: [(&[u8], Error); 2] = [
    (
        &[0],
        Error::Truncated {
            actual: 1,
            required: 2,
        },
    ),
    (
        &[0, 3, 7],
        Error::Truncated {
            actual: 3,
            required: 5,
        },
    ),
];
