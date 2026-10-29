use std::net::SocketAddr;

use dope_manifold::service::{self, Endpoint, Revision, Snapshot};
use dope_test::alloc::Tracker;
use o3::depot::array;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn reconcile_policy_layout() {
    assert_eq!(
        std::mem::align_of::<service::sync::Preserve>(),
        std::mem::align_of::<service::sync::Replace>()
    );
    assert_eq!(std::mem::size_of::<service::sync::Preserve>(), 0);
    assert_eq!(std::mem::size_of::<service::sync::Replace>(), 0);
}

#[test]
fn accepts_endpoint_limit() {
    let snap = Snapshot::<_, { service::MAX_ENDPOINTS }>::from_vec(
        Revision::new(1),
        array::Vec::from_fn(array::Len::MAX, |slot| {
            let index = slot.index();
            Endpoint::new(index, index)
        }),
    );

    assert_eq!(snap.endpoints().len(), service::MAX_ENDPOINTS);
}

#[test]
fn snapshot_no_alloc() {
    let first = SocketAddr::from(([127, 0, 0, 1], 10_016));
    let second = SocketAddr::from(([127, 0, 0, 1], 10_017));

    let (single, one_mem) =
        Tracker::usage(|| Snapshot::<_>::from_one(Revision::new(1), Endpoint::new(first, first)));
    let (pair, pair_mem) = Tracker::usage(|| {
        Snapshot::from_vec(
            Revision::new(1),
            array::Vec::from([Endpoint::new(first, first), Endpoint::new(second, second)]),
        )
    });

    assert_eq!(one_mem, (0, 0));
    assert_eq!(single.endpoints().len(), 1);
    assert_eq!(pair_mem, (0, 0));
    assert_eq!(pair.endpoints().len(), 2);
}
