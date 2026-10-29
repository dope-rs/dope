use std::num::NonZero;

use dope_test::quic::{
    Datagram, EncodeError, FlowOracle, HEADER_BYTES, Observation, Oracle, ShapeError, Violation,
    net,
};
use o3::alloc;

#[test]
fn shape_bounds() {
    assert_eq!(
        Datagram::try_new(0, 0, 0),
        Err(ShapeError::Flows { actual: 0 })
    );
    assert_eq!(
        Datagram::try_new(1, 0, 0),
        Err(ShapeError::PerFlow { actual: 0 })
    );
    assert_eq!(
        Datagram::try_new(1, 1, HEADER_BYTES - 1),
        Err(ShapeError::Payload {
            actual: HEADER_BYTES - 1,
            required: HEADER_BYTES,
        })
    );
    assert_eq!(
        Datagram::try_new(usize::MAX, 2, HEADER_BYTES),
        Err(ShapeError::Count {
            flows: usize::MAX,
            per_flow: 2,
        })
    );
    assert_eq!(
        Datagram::try_new(1, 2, usize::MAX),
        Err(ShapeError::Bytes {
            count: 2,
            payload: usize::MAX,
        })
    );
}

#[test]
fn complete_once() {
    let scenario = (Datagram::try_new(2, 3, 32)).expect("the scenario dimensions are valid");
    let mut oracle = Oracle::new(scenario);
    for flow in 0..scenario.flows() {
        for sequence in 0..scenario.per_flow() {
            let payload =
                (scenario.try_payload(flow, sequence)).expect("a scenario identity must encode");
            let seen =
                (oracle.observe(&payload)).expect("a generated payload must satisfy its scenario");
            assert_eq!((seen.flow, seen.sequence), (flow, sequence));
        }
    }
    let first = (scenario.try_payload(0, 0)).expect("the first scenario identity must encode");
    std::assert_matches!(
        oracle.observe(&first),
        Err(Violation::Duplicate {
            flow: 0,
            sequence: 0,
        })
    );
    assert!(oracle.is_complete());
    let complete = (oracle.finish()).expect("every scenario identity was delivered");
    assert_eq!(complete.delivered(), scenario.total());
    assert_eq!(complete.bytes(), scenario.total() * 32);
}

#[test]
fn encoding_bounds() {
    let scenario = (Datagram::try_new(1, 1, 17)).expect("the scenario dimensions are valid");
    let mut payload = [0; 17];
    assert!(
        scenario.encode(0, 0, &mut payload).is_ok(),
        "the sole scenario identity and exact buffer must encode"
    );
    let seen = (Oracle::new(scenario).observe(&payload))
        .expect("an encoded payload must satisfy its scenario");
    assert_eq!(seen.flow, 0);
    std::assert_matches!(
        scenario.encode(1, 0, &mut payload),
        Err(EncodeError::Flow {
            actual: 1,
            count: 1,
        })
    );
    std::assert_matches!(
        scenario.encode(0, 1, &mut payload),
        Err(EncodeError::Sequence {
            actual: 1,
            count: 1,
        })
    );
    std::assert_matches!(
        scenario.encode(0, 0, &mut payload[..16]),
        Err(EncodeError::Length {
            actual: 16,
            expected: 17,
        })
    );
    std::assert_matches!(
        scenario.try_payload(1, 0),
        Err(EncodeError::Flow {
            actual: 1,
            count: 1,
        })
    );
    std::assert_matches!(
        scenario.try_payload(0, 1),
        Err(EncodeError::Sequence {
            actual: 1,
            count: 1,
        })
    );
}

#[test]
fn rejects_huge_payload() {
    let scenario = Datagram::try_new(1, 1, usize::MAX)
        .expect("one payload has a representable total byte count");
    std::assert_matches!(
        scenario.try_payload(0, 0),
        Err(EncodeError::Allocation(alloc::Error::Overflow))
    );
}

#[test]
fn transport_conservation() {
    let batch = (NonZero::new(8)).expect("eight datagrams are nonzero");
    let mut oracle = net::Oracle::default();
    std::assert_matches!(
        oracle.accept(batch, 0, true),
        Err(net::Violation::EmptyAcceptance { datagrams, bytes: 0 }) if datagrams == batch
    );
    std::assert_matches!(
        oracle.accept(batch, 9600, false),
        Err(net::Violation::InvalidSegmentation { datagrams, is_gso: false }) if datagrams == batch
    );
    assert!(oracle.is_complete());
    assert!(
        oracle.accept(NonZero::<usize>::MIN, 1200, false).is_ok(),
        "one datagram is an unsegmented ownership transfer"
    );
    assert!(
        oracle.accept(batch, 9600, true).is_ok(),
        "eight datagrams are a segmented ownership transfer"
    );
    std::assert_matches!(
        oracle.settle(1200, net::Outcome::Sent(1199)),
        Err(net::Violation::PartialDatagram {
            owner_bytes: 1200,
            sent_bytes: 1199,
        })
    );
    assert!(
        oracle.settle(1200, net::Outcome::Sent(1200)).is_ok(),
        "the first owner settles its exact byte count"
    );
    std::assert_matches!(
        oracle.finish(),
        Err(net::Violation::Incomplete {
            outstanding_operations: 1,
            outstanding_bytes: 9600,
        })
    );
    assert!(
        oracle.settle(9600, net::Outcome::Sent(9600)).is_ok(),
        "the segmented owner settles its exact byte count"
    );
    assert!(oracle.is_complete());
    let done = (oracle.finish()).expect("every accepted owner was settled");
    assert_eq!(done.operations(), 2);
    assert_eq!(done.datagrams(), 9);
    assert_eq!(done.gso_ops(), 1);
    assert_eq!(done.saved_ops(), 7);
    assert_eq!(done.max_batch(), 8);
    assert_eq!(done.peak_bytes(), 10_800);
}

#[test]
fn keeps_transport_bounds() {
    let mut oracle = net::Oracle::default();
    let batch = NonZero::new(usize::MAX - 1).expect("nonzero batch");
    oracle
        .accept(batch, usize::MAX - 1, true)
        .expect("admitted batch");
    std::assert_matches!(
        oracle.accept(NonZero::<usize>::MIN, 2, false),
        Err(net::Violation::AcceptedBytesOverflow { .. })
    );
    std::assert_matches!(
        oracle.accept(NonZero::new(2).expect("two datagrams"), 1, true),
        Err(net::Violation::AcceptedDatagramsOverflow { .. })
    );
    oracle
        .accept(NonZero::<usize>::MIN, 1, false)
        .expect("remaining capacity");
    oracle
        .settle(usize::MAX - 1, net::Outcome::Shutdown)
        .expect("batch retired");
    std::assert_matches!(
        oracle.settle(2, net::Outcome::Failed),
        Err(net::Violation::OutstandingBytesUnderflow { .. })
    );
    oracle
        .settle(1, net::Outcome::Failed)
        .expect("remaining owner retired");
    let done = oracle.finish().expect("all owners retired");
    assert_eq!((done.failed_ops(), done.shutdowns()), (1, 1));
    assert_eq!(done.saved_ops(), usize::MAX - 2);
}

#[test]
fn bitset_word_boundary() {
    let scenario =
        (Datagram::try_new(1, 65, HEADER_BYTES)).expect("the scenario dimensions are valid");
    let mut oracle = Oracle::new(scenario);
    let at_63 = (scenario.try_payload(0, 63)).expect("sequence 63 must encode");
    let at_64 = (scenario.try_payload(0, 64)).expect("sequence 64 must encode");

    let seen_63 = (oracle.observe(&at_63)).expect("sequence 63 must enter the first bitset word");
    assert_eq!(seen_63.sequence, 63);
    let seen_64 = (oracle.observe(&at_64)).expect("sequence 64 must enter the second bitset word");
    assert_eq!(seen_64.sequence, 64);
    std::assert_matches!(
        oracle.observe(&at_63),
        Err(Violation::Duplicate {
            flow: 0,
            sequence: 63,
        })
    );
    assert_eq!(oracle.delivered(), 2);
}

#[test]
fn flow_state_is_disjoint() {
    let scenario = (Datagram::try_new(2, 3, 24)).expect("the scenario dimensions are valid");
    for flow in 0..scenario.flows() {
        let mut oracle = (FlowOracle::try_new(scenario, flow))
            .expect("each enumerated flow belongs to the scenario");
        for sequence in 0..scenario.per_flow() {
            let payload = (scenario.try_payload(flow, sequence))
                .expect("each enumerated identity must encode");
            let seen = (oracle.observe(&payload))
                .expect("a generated payload must satisfy its bound flow");
            assert_eq!((seen.flow, seen.sequence), (flow, sequence));
        }
        assert_eq!(oracle.delivered(), scenario.per_flow());
        let done = (oracle.finish()).expect("every identity of the bound flow was delivered");
        assert_eq!(done.delivered(), scenario.per_flow());
        assert_eq!(done.bytes(), scenario.per_flow() * 24);
    }
}

#[test]
fn bind_records_first() {
    let scenario = (Datagram::try_new(3, 2, 32)).expect("the scenario dimensions are valid");
    let first = (scenario.try_payload(2, 0)).expect("the first identity of flow two must encode");
    let (mut oracle, seen) = (FlowOracle::bind(scenario, &first))
        .expect("a generated first payload must bind its encoded flow");
    assert_eq!(seen.flow, 2);
    assert_eq!(seen.sequence, 0);
    assert_eq!(oracle.delivered(), 1);
    std::assert_matches!(
        oracle.observe(&first),
        Err(Violation::Duplicate {
            flow: 2,
            sequence: 0,
        })
    );
    let second = (scenario.try_payload(2, 1)).expect("the second identity of flow two must encode");
    let seen =
        (oracle.observe(&second)).expect("the generated second payload must extend its bound flow");
    assert_eq!((seen.flow, seen.sequence), (2, 1));
    assert_eq!(oracle.delivered(), scenario.per_flow());
    let done = (oracle.finish()).expect("every identity of the bound flow was delivered");
    assert_eq!(done.delivered(), 2);
}

#[test]
fn missing_keeps_flow() {
    let scenario = Datagram::try_new(2, 65, HEADER_BYTES).expect("valid scenario");
    let mut oracle = FlowOracle::try_new(scenario, 1).expect("second flow");
    let payload = scenario.try_payload(1, 63).expect("valid identity");
    oracle.observe(&payload).expect("out-of-order payload");
    for sequence in (0..63).chain(64..65) {
        assert_eq!(oracle.missing(), Some(Observation { flow: 1, sequence }));
        let payload = scenario.try_payload(1, sequence).expect("valid identity");
        oracle.observe(&payload).expect("distinct payload");
    }
    assert_eq!(oracle.missing(), None);
    let done = oracle.finish().expect("all identities remain recorded");
    assert_eq!(done.delivered(), scenario.per_flow());
}
