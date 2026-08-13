use std::task;

use dope::manifold::file::durable::{self, Appender, CommitOutcome, Failure, Manifold, Ticket};
use dope_fiber::{abi::Fiber, context::PollCall};
use dope_test::{checks::TrackingAlloc, fibers, file, scenario::rt::Runtime};

#[global_allocator]
static ALLOCATOR: TrackingAlloc = TrackingAlloc::new();

const ID: u8 = 37;
const BLOCKS: usize = 2;
const BLOCK_BYTES: usize = 64;

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
struct Host<'d> {
    #[pin]
    #[manifold]
    durable: Manifold<'d, ID, BLOCKS, BLOCK_BYTES>,
    #[dispatcher(marker)]
    driver: ::core::marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

struct Commits<'d> {
    appender: &'d Appender<'d, ID, BLOCKS, BLOCK_BYTES>,
    first: Ticket<'d, ID>,
    second: Ticket<'d, ID>,
    first_done: bool,
    second_done: bool,
}

impl<'d> Commits<'d> {
    fn poll_one(
        appender: &'d Appender<'d, ID, BLOCKS, BLOCK_BYTES>,
        ticket: &mut Ticket<'d, ID>,
        call: &std::pin::Pin<&mut dope_fiber::context::Context<'_, 'd>>,
    ) -> Option<Result<(), Failure>> {
        match appender.poll_commit(ticket, call.as_ref().completion_waker()) {
            CommitOutcome::Pending => None,
            CommitOutcome::Done(result) => Some(result),
            CommitOutcome::Expired => panic!("live durable ticket expired"),
        }
    }
}

impl<'d> Fiber<'d> for Commits<'d> {
    type Output = Result<(), Failure>;

    fn poll(call: PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, context) = call.into_parts();
        let this = this.get_mut();
        if !this.first_done
            && let Some(result) = Self::poll_one(this.appender, &mut this.first, &context)
        {
            result?;
            this.first_done = true;
        }
        if !this.second_done
            && let Some(result) = Self::poll_one(this.appender, &mut this.second, &context)
        {
            result?;
            this.second_done = true;
        }
        if !this.first_done || !this.second_done {
            return task::Poll::Pending;
        }
        assert!(matches!(
            this.appender
                .poll_commit(&mut this.first, context.as_ref().completion_waker()),
            CommitOutcome::Expired
        ));
        task::Poll::Ready(Ok(()))
    }
}

#[test]
fn batched_tickets_complete_only_after_the_durability_barrier_without_allocating() {
    let source = file::File::with("durable_append", b"prefix:");
    let factory = durable::Factory::<ID, BLOCKS, BLOCK_BYTES>::open(source.path(), 2)
        .expect("durable factory");

    Runtime::throughput()
        .executor()
        .with_factory(factory)
        .try_enter(|mut session| {
            let appender = session.storage();
            let ((first, second), allocation) = TrackingAlloc::<0>::measure(|| {
                (
                    appender.try_append(b"one").expect("first ticket"),
                    appender.try_append(b"two").expect("second ticket"),
                )
            });
            assert_eq!(allocation, (0, 0));

            session
                .with_app(
                    Host {
                        durable: appender.manifold(),
                        driver: ::core::marker::PhantomData,
                    },
                    |mut app| {
                        fibers::TEST
                            .drive(
                                &mut app,
                                Commits {
                                    appender,
                                    first,
                                    second,
                                    first_done: false,
                                    second_done: false,
                                },
                            )
                            .expect("durable commits");
                    },
                )
                .expect("durable app teardown");
        })
        .expect("durable storage");

    assert_eq!(
        std::fs::read(source.path()).expect("durable file"),
        b"prefix:onetwo"
    );
}
