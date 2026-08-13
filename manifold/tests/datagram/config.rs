use std::io;

use dope_manifold::datagram::Config;

#[test]
fn send_bounds_are_progress_capable_by_construction() {
    for (pending, in_flight) in [(0, 1), (usize::MAX, 1), (1, 0), (1, usize::MAX)] {
        let error = Config::new(pending, 1, in_flight)
            .expect_err("invalid send capacity was representable");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    Config::new(2, 3, 4).expect("valid send capacities");
}
