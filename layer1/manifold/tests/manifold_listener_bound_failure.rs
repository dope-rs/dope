use std::pin;

use dope_manifold::network::listener::conn::write::body::Failure;
use dope_net::link::egress::Load;
use o3::{
    base::cell::region,
    buffer::resident::{self, bound},
};

#[test]
fn releases_bound_owner() {
    region::Token::scope_pinned(pin::pin!(resident::Domain::new()), |token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(64, token.proof())
            .expect("resident budget");
        let owner = resident::Exact::try_copy(&budget, b"bound")
            .expect("resident owner")
            .into_shared();
        let load = Load::new(owner);
        let expected = load.charge();
        let rejected = load
            .try_bound::<bound::AtMost<0>>()
            .err()
            .expect("retained owner exceeds zero bound");
        assert!(matches!(
            (Failure::from(rejected), budget.resident()),
            (Failure::Bound { charge, maximum: 0 }, 0) if charge == expected
        ));
    });
}

#[test]
fn mismatch_releases_owners() {
    use dope_core::io::transfer;
    use dope_manifold::network::listener::conn::write::{self, error};
    use dope_net::link::egress::{load, reserve};
    use reserve::{
        error::{Cause, Mismatch},
        limit,
    };

    region::Token::scope_pinned(pin::pin!(resident::Domain::new()), |token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(64, token.proof())
            .expect("resident budget");
        let owner = resident::Exact::try_copy(&budget, b"owner")
            .expect("resident owner")
            .into_shared();
        let body = load::Value::Unchecked(Load::new(owner));
        let rejected = error::Mismatch::Values {
            cause: Cause::Changed(Mismatch::First(limit::Mismatch::Maximum {
                maximum: transfer::NonZero::fixed_size::<1>(),
                actual: transfer::NonZero::fixed_size::<5>(),
            })),
            expected: reserve::Bounds::one(limit::Limit::at_most::<1, 64>()),
            first: error::Part::Body(body),
            second: error::Part::Empty,
            receipt: 17,
            end: write::End::Close,
        };
        let (cause, receipt) = rejected.into_parts();
        assert_eq!((budget.resident(), receipt), (0, 17));
        assert!(
            match cause {
                error::Cause::Values {
                    cause:
                        Cause::Changed(Mismatch::First(limit::Mismatch::Maximum { maximum, actual })),
                    ..
                } => maximum.widen().get() == 1 && actual.widen().get() == 5,
                _ => false,
            },
            "mismatch cause changed"
        );
    });
}
