use std::pin;

use o3::{base::cell::region, buffer::resident};

pub fn try_with_budget<R>(
    capacity: usize,
    run: impl for<'d> FnOnce(resident::Budget<'d>) -> R,
) -> Result<R, resident::Reuse> {
    region::Token::scope_pinned(pin::pin!(resident::Domain::new()), |token, domain| {
        let budget = domain.get_mut().try_budget(capacity, token.proof())?;
        Ok(run(budget))
    })
}
