use crate::common::rt::{finish, recover};

use dope_core::{
    driver::life::recovery,
    io::socket::{install::Install, option::Tuning},
};

use crate::common::driver::{Scoped as _, support::Targets as _, tuning::Tuned as _};

pub mod common;

use dope_core::{
    driver::{
        route::{FrameworkTag, KeyTag, Tag, kind},
        settings,
    },
    io::{fd::handles, socket::option},
};
use dope_test::reclaim::Reports;

use crate::common::rt::Runtime;

#[test]
fn tracks_fixed_slots() {
    let setting = option::Buffer::try_from(4096_usize).expect("socket buffer size");
    let runtime = Runtime::quic::<2, 8>().file_slots(settings::FileSlots::fixed::<0, 2>());
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    runtime.with_scope(
        |scope| {
            scope
                .with_flights::<KeyTag<1>, 2>(|mut driver, turn, flights| {
                    let options =
                        option::Set::new(Some(setting), None, None, None, None, None, None);

                    let sockets: [_; 2] = std::array::from_fn(|_| {
                        let (socket, _) = Install::bind_udp(
                            driver.reborrow(),
                            ([127, 0, 0, 1], 0).into(),
                            common::DATAGRAM,
                        )
                        .expect("bound datagram socket");
                        handles::Descriptor::from(socket)
                    });
                    assert_ne!(sockets[0].index(), sockets[1].index());
                    let submissions = sockets.map(|socket| {
                        let index = socket.index();
                        let owner = driver.driver_ref().tuned_target::<KeyTag<1>>(index);
                        let target = owner.operation::<kind::Tuning>();
                        let tuning = Tuning::try_new(
                            &mut driver.reborrow(),
                            &flights,
                            owner,
                            socket,
                            options,
                        )
                        .map_err(|rejected| rejected.into_parts().1)
                        .expect("tuning submission");
                        (index, target, tuning)
                    });
                    let mut pending = submissions.map(|(index, target, tuning)| match tuning {
                        Tuning::Applied(socket) => {
                            assert_eq!(socket.index(), index);
                            socket.close(&mut driver);
                            None
                        }
                        Tuning::Pending(wait) => Some((index, target, wait)),
                    });
                    while pending.iter().any(Option::is_some) {
                        let done = driver.wait_tuning(turn).ok().expect("tuning completion");
                        let entry = pending
                            .iter_mut()
                            .find(|entry| {
                                entry
                                    .as_ref()
                                    .is_some_and(|(_, target, _)| target.matches(done.token()))
                            })
                            .expect("completion addresses one live transaction");
                        let (index, _, wait) = entry.take().expect("pending transaction");
                        assert_eq!(wait.fd().index(), index);
                        let (socket, outcome) = wait
                            .complete(done)
                            .ok()
                            .expect("batch completion must match its authority");
                        assert_eq!(outcome, Ok(()));
                        assert_eq!(socket.index(), index);
                        socket.close(&mut driver);
                    }
                })
                .expect("flight fixture");
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.events().expect("valid reclamation").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}
