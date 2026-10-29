pub mod common;

use std::convert::Infallible;

use common::discard::Observed;
use dope_core::{
    driver::{
        life::recovery,
        route::{FrameworkTag, Tag, kind},
        settings::{Config, FileSlots},
    },
    io::{
        Reclaimed,
        event::{batch::Flow, net::accept::Failure},
        transfer,
    },
};
use dope_net::{
    link::{event::SendOutcome, slot::transmit::Attempt},
    wire::send::reclaim::Advance,
};
use dope_test::{reclaim::Reports, scope};

#[test]
fn discard_stops_send() {
    let config = Config::default().with_files(FileSlots::fixed::<1, 1>());
    let mut reports = Reports::new(config, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    let observed = Observed::capture(
        config,
        |settled| {
            let closed = matches!(&settled, SendOutcome::Close(_));
            match settled {
                SendOutcome::Rejected { failure, .. } => {
                    assert!(closed, "settle rejected: {failure:?}")
                }
                SendOutcome::Failed { failure, .. } => {
                    assert!(closed, "settle failed: {failure:?}")
                }
                SendOutcome::Invalid { error, .. } => assert!(closed, "settle invalid: {error:?}"),
                _ => assert!(closed, "discarded send did not close"),
            }
        },
        recovery::Policy::new(
            |event| reports.push(event),
            |failure: recovery::Recovery<'_, '_>| -> Infallible {
                panic!("driver recovery: {:?}", failure.cause())
            },
        ),
        |result| {
            result
                .as_ref()
                .map_err(scope::Error::cause)
                .expect("native lifecycle");
            result
                .ok()
                .expect("driver output")
                .expect("discard fixture")
        },
    );
    assert_eq!(observed.sent, Ok(transfer::Len::fixed::<1>()));
    let idle = matches!(&observed.resubmit, Attempt::Quiescent(Advance::Idle));
    match observed.resubmit {
        Attempt::Rejected(cause, _) => assert!(idle, "resubmit rejected: {cause:?}"),
        Attempt::Invalid(cause) => assert!(idle, "resubmit invalid: {cause:?}"),
        _ => assert!(idle, "discarded submission was not quiescent"),
    }
    assert!(observed.buffered);
    assert!(!observed.inflight);
    let events = reports.events().expect("reclaimed operations");
    assert_eq!(events.len(), usize::from(observed.pending_accept));
    for event in events {
        assert!(
            matches!(event,
                Reclaimed::Accept {
                    result: Err(Failure::Syscall(error)), flow: Flow::Last, ..
                } if error.is_canceled()
            ),
            "unexpected reclaimed operation: {event:?}"
        );
    }
}
