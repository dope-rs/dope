use std::cell::{Cell, RefCell};
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::task::Poll;

use dope::manifold::Manifold;
use dope::runtime::Executor;
use dope_fiber::{AppSessionExt, Context, Fiber};
use dope_test::{CountDrop, OrderedDrop, assert_panics_with, poll_ready, with_context};

#[test]
#[expect(clippy::needless_return)]
fn completed_generated_fiber_repoll_panics() {
    with_context(|mut cx| {
        let mut plain = pin!(dope_gen::fiber!('_ => async move {
            dope_fiber::ready(9usize).await
        }));
        assert_eq!(poll_ready(plain.as_mut(), cx.as_mut()), 9);
        assert_panics_with(
            || {
                let _ = Fiber::poll(plain.as_mut(), cx.as_mut());
            },
            "after completion",
        );

        let mut residual = pin!(dope_gen::fiber!('_ => async move {
            let _: usize = dope_fiber::ready(Err::<usize, u8>(7)).await?;
            Ok::<usize, u8>(1)
        }));
        assert_eq!(poll_ready(residual.as_mut(), cx.as_mut()), Err(7));
        assert_panics_with(
            || {
                let _ = Fiber::poll(residual.as_mut(), cx.as_mut());
            },
            "after completion",
        );

        let mut returned = pin!(dope_gen::fiber!('_ => async move {
            dope_fiber::ready(()).await;
            return 13usize;
        }));
        assert_eq!(poll_ready(returned.as_mut(), cx.as_mut()), 13);
        assert_panics_with(
            || {
                let _ = Fiber::poll(returned.as_mut(), cx.as_mut());
            },
            "after completion",
        );
    });
}

#[test]
fn continuation_panic_poisons_generated_fiber() {
    with_context(|mut cx| {
        let mut fiber = pin!(dope_gen::fiber!('_ => async move {
            dope_fiber::ready(()).await;
            ::core::panic!("continuation panic")
        }));
        assert_panics_with(
            || {
                let _ = Fiber::poll(fiber.as_mut(), cx.as_mut());
            },
            "continuation panic",
        );
        assert_panics_with(
            || {
                let _ = Fiber::poll(fiber.as_mut(), cx.as_mut());
            },
            "after panic",
        );
    });
}

struct DropPanic<'a> {
    drops: &'a Cell<usize>,
}

impl Drop for DropPanic<'_> {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
        panic!("child drop panic");
    }
}

impl<'a, 'd> Fiber<'d> for DropPanic<'a> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        Poll::Ready(())
    }
}

struct DropPanicMaker<'a>(&'a Cell<usize>);

impl<'a> dope_fiber::__private::ScopedFactory<(), DropPanic<'a>> for DropPanicMaker<'a> {
    unsafe fn make(self, _: *mut ()) -> DropPanic<'a> {
        DropPanic { drops: self.0 }
    }
}

#[test]
fn scoped_child_drop_panic_cannot_redrop_child() {
    with_context(|mut cx| {
        let drops = Cell::new(0);
        let context = unsafe { Pin::get_unchecked_mut(cx.as_mut()) as *mut Context<'_, '_> };
        let mut fiber = pin!(dope_fiber::__private::Scoped::new(
            context,
            (),
            DropPanicMaker(&drops),
        ));
        assert_panics_with(
            || {
                let _ = Fiber::poll(fiber.as_mut(), cx.as_mut());
            },
            "child drop panic",
        );
        assert_eq!(drops.get(), 1);
        assert_panics_with(
            || {
                let _ = Fiber::poll(fiber.as_mut(), cx.as_mut());
            },
            "after panic",
        );
        assert_eq!(drops.get(), 1);
    });
}

#[test]
fn generated_await_drop_panic_cannot_redrop_child() {
    with_context(|mut cx| {
        let drops = Cell::new(0);
        let captured = &drops;
        let mut fiber = pin!(dope_gen::fiber!('_ => async move {
            DropPanic { drops: captured }.await;
        }));
        assert_panics_with(
            || {
                let _ = Fiber::poll(fiber.as_mut(), cx.as_mut());
            },
            "child drop panic",
        );
        assert_eq!(drops.get(), 1);
        assert_panics_with(
            || {
                let _ = Fiber::poll(fiber.as_mut(), cx.as_mut());
            },
            "after panic",
        );
        assert_eq!(drops.get(), 1);
    });
}

struct OwnedReceiver(String);

impl OwnedReceiver {
    fn consume<'d>(self) -> impl Fiber<'d, Output = usize> + use<'d> {
        dope_fiber::ready(self.0.len())
    }
}

struct MutableReceiver(usize);

struct Increment<'a>(&'a mut MutableReceiver);

impl<'a, 'd> Fiber<'d> for Increment<'a> {
    type Output = usize;

    fn poll(self: Pin<&mut Self>, _: Pin<&mut Context<'_, 'd>>) -> Poll<usize> {
        let this = self.get_mut();
        this.0.0 += 1;
        Poll::Ready(this.0.0)
    }
}

impl MutableReceiver {
    fn increment<'a, 'd>(&'a mut self) -> impl Fiber<'d, Output = usize> + use<'a, 'd> {
        Increment(self)
    }
}

struct LiveGuard(Rc<Cell<bool>>);

impl Drop for LiveGuard {
    fn drop(&mut self) {
        self.0.set(false);
    }
}

struct ObserveLive(Rc<Cell<bool>>);

impl<'d> Fiber<'d> for ObserveLive {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _: Pin<&mut Context<'_, 'd>>) -> Poll<()> {
        assert!(self.0.get(), "guard dropped before child poll");
        Poll::Ready(())
    }
}

struct ObserveDropped(Rc<Cell<bool>>);

impl<'d> Fiber<'d> for ObserveDropped {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _: Pin<&mut Context<'_, 'd>>) -> Poll<()> {
        assert!(!self.0.get(), "explicitly moved guard remained live");
        Poll::Ready(())
    }
}

fn borrow_value<'a, 'd>(value: &'a str) -> impl Fiber<'d, Output = &'a str> + use<'a, 'd> {
    dope_fiber::ready(value)
}

#[dope_gen::fiber_fn('d)]
async fn generated_prefix_argument<'d>(__dope_local_0_value: usize) -> usize {
    dope_fiber::ready(__dope_local_0_value + 1).await
}

#[test]
#[expect(clippy::let_and_return)]
fn branch_tails_and_if_let_bindings_preserve_values() {
    with_context(|mut cx| {
        let mut fiber = pin!(dope_gen::fiber!('_ => async move {
            let base = dope_fiber::ready(4usize).await;
            let branch = if base == 4 {
                let value = dope_fiber::ready(base + 1).await;
                value * 2
            } else {
                let value = dope_fiber::ready(0usize).await;
                value
            };
            let matched = match branch {
                10 => {
                    let value = dope_fiber::ready(7usize).await;
                    value + 1
                }
                _ => {
                    let value = dope_fiber::ready(0usize).await;
                    value
                }
            };
            let scrutinee = match dope_fiber::ready(matched).await {
                8 => dope_fiber::ready(1usize).await,
                _ => dope_fiber::ready(0usize).await,
            };
            if let Some(text) = Some(String::from("native")) {
                let length = dope_fiber::ready(text.len()).await;
                matched + length + scrutinee
            } else {
                0
            }
        }));
        assert_eq!(poll_ready(fiber.as_mut(), cx.as_mut()), 15);
    });
}

#[test]
fn owned_capture_receiver_and_borrowed_output_are_supported() {
    let text = String::from("borrowed");
    with_context(|mut cx| {
        let captured = OwnedReceiver(String::from("receiver"));
        let mut owned = pin!(dope_gen::fiber!('_ => async move {
            captured.consume().await
        }));
        assert_eq!(poll_ready(owned.as_mut(), cx.as_mut()), 8);

        let text = text.as_str();
        let mut borrowed = pin!(dope_gen::fiber!('_ => async move {
            borrow_value(text).await
        }));
        assert_eq!(poll_ready(borrowed.as_mut(), cx.as_mut()), "borrowed");
    });
}

#[test]
fn generated_identifiers_do_not_capture_caller_names() {
    with_context(|mut cx| {
        let mut fiber = pin!(dope_gen::fiber!('_ => async move {
            let __dope_brand = 1usize;
            let __dope_future = 2usize;
            let value = dope_fiber::ready(Ok::<usize, u8>(7)).await?;
            Ok::<usize, u8>(__dope_brand + __dope_future + value)
        }));
        assert_eq!(poll_ready(fiber.as_mut(), cx.as_mut()), Ok(10));

        let mut prefixed = pin!(generated_prefix_argument(8));
        assert_eq!(poll_ready(prefixed.as_mut(), cx.as_mut()), 9);
    });
}

#[test]
fn lexical_drop_guard_survives_until_await_completion() {
    with_context(|mut cx| {
        let live = Rc::new(Cell::new(true));
        let observed = Rc::clone(&live);
        let mut lexical = pin!(dope_gen::fiber!('_ => async move {
            let _guard = LiveGuard(Rc::clone(&live));
            ObserveLive(Rc::clone(&live)).await;
        }));
        poll_ready(lexical.as_mut(), cx.as_mut());
        assert!(!observed.get());

        let live = Rc::new(Cell::new(true));
        let mut explicit = pin!(dope_gen::fiber!('_ => async move {
            let guard = LiveGuard(Rc::clone(&live));
            ::core::mem::drop(guard);
            ObserveDropped(Rc::clone(&live)).await;
        }));
        poll_ready(explicit.as_mut(), cx.as_mut());
    });
}

#[test]
fn sequential_awaits_support_distinct_types() {
    with_context(|mut cx| {
        let mut fiber = pin!(dope_gen::fiber!('_ => async move {
            let first: u8 = dope_fiber::ready(3u8).await;
            let second: u16 = dope_fiber::ready(5u16).await;
            usize::from(first) + usize::from(second)
        }));
        assert_eq!(poll_ready(fiber.as_mut(), cx.as_mut()), 8);
    });
}

#[test]
#[expect(clippy::needless_borrow)]
fn consuming_and_mutable_receivers_resume_with_ownership() {
    with_context(|mut cx| {
        let mut consuming = pin!(dope_gen::fiber!('_ => async move {
            let mut receiver = OwnedReceiver(String::from("first"));
            let first = receiver.consume().await;
            receiver = OwnedReceiver(String::from("second"));
            first + receiver.consume().await
        }));
        assert_eq!(poll_ready(consuming.as_mut(), cx.as_mut()), 11);

        let mut borrowed = pin!(dope_gen::fiber!('_ => async move {
            let mut receiver = MutableReceiver(0);
            let first = (&mut receiver).increment().await;
            let second = (&mut receiver).increment().await;
            first + second
        }));
        assert_eq!(poll_ready(borrowed.as_mut(), cx.as_mut()), 3);
    });
}

#[derive(Clone, Copy)]
struct Handle(usize);

impl Handle {
    fn wait<'a, 'd>(&'a self) -> impl Fiber<'d, Output = usize> + 'a {
        dope_fiber::ready(self.0)
    }

    fn result<'a, 'd>(&'a self) -> impl Fiber<'d, Output = Result<usize, std::io::Error>> + 'a {
        dope_fiber::ready(Ok(self.0))
    }
}

#[test]
fn borrowed_handles_stay_live_across_interleaved_awaits() {
    with_context(|mut cx| {
        let first = Handle(1);
        let second = Handle(10);
        let mut fiber = pin!(dope_gen::fiber!('_ => async move {
            let mut sum = first.wait().await;
            sum += second.wait().await;
            sum += first.wait().await;
            sum + first.result().await.expect("result")
        }));
        assert_eq!(poll_ready(fiber.as_mut(), cx.as_mut()), 13);
    });
}

#[test]
fn sequential_storage_tracks_max_live_size() {
    let one = dope_gen::fiber!('_ => async move {
        {
            let storage: [u8; 4096] = [1; 4096];
            dope_fiber::ready(()).await;
            storage[0] as usize
        }
    });
    let sequential = dope_gen::fiber!('_ => async move {
        let first = {
            let storage: [u8; 4096] = [1; 4096];
            dope_fiber::ready(()).await;
            storage[0] as usize
        };
        let second = {
            let storage: [u8; 4096] = [2; 4096];
            dope_fiber::ready(()).await;
            storage[0] as usize
        };
        first + second
    });
    assert!(std::mem::size_of_val(&sequential) <= std::mem::size_of_val(&one) + 64);
}

#[test]
fn loop_await_break_and_continue_complete_in_one_poll() {
    with_context(|mut cx| {
        let steps = Rc::new(Cell::new(0usize));
        let observed = Rc::clone(&steps);
        let mut fiber = pin!(dope_gen::fiber!('_ => async move {
            let mut sum = 0usize;
            loop {
                let value = steps.get();
                steps.set(value + 1);
                dope_fiber::ready(()).await;
                if value.is_multiple_of(2) {
                    continue;
                }
                sum += value;
                if value == 65 {
                    break;
                }
            }
            sum
        }));
        assert_eq!(Fiber::poll(fiber.as_mut(), cx.as_mut()), Poll::Ready(1089));
        assert_eq!(observed.get(), 66);
    });
}

#[test]
fn completed_iterations_drop_before_ready_return() {
    with_context(|mut cx| {
        let drops = Rc::new(Cell::new(0usize));
        let observed = Rc::clone(&drops);
        {
            let mut fiber = pin!(dope_gen::fiber!('_ => async move {
                let mut iteration = 0usize;
                loop {
                    let _guard = CountDrop(Rc::clone(&drops));
                    dope_fiber::ready(()).await;
                    iteration += 1;
                    if iteration == 100 {
                        break;
                    }
                }
            }));
            assert_eq!(Fiber::poll(fiber.as_mut(), cx.as_mut()), Poll::Ready(()));
            assert_eq!(observed.get(), 100);
        }
        assert_eq!(observed.get(), 100);
    });
}

#[allow(unused_assignments)]
#[test]
#[expect(clippy::never_loop)]
fn loop_assignment_preserves_drop_order() {
    with_context(|mut cx| {
        let order = Rc::new(RefCell::new(Vec::new()));
        let observed = Rc::clone(&order);
        let mut fiber = pin!(dope_gen::fiber!('_ => async move {
            let mut guard = OrderedDrop {
                order: Rc::clone(&order),
                value: 1,
            };
            loop {
                order.borrow_mut().push(2);
                guard = OrderedDrop {
                    order: Rc::clone(&order),
                    value: 3,
                };
                order.borrow_mut().push(4);
                dope_fiber::ready(()).await;
                break;
            }
            ::core::mem::drop(guard);
        }));
        poll_ready(fiber.as_mut(), cx.as_mut());
        assert_eq!(&*observed.borrow(), &[2, 1, 4, 3]);
    });
}

#[test]
fn while_await_runs_to_completion() {
    with_context(|mut cx| {
        let mut fiber = pin!(dope_gen::fiber!('_ => async move {
            let mut values = [1usize, 2, 3, 4].into_iter();
            let mut sum = 0usize;
            while let Some(value) = dope_fiber::ready(values.next()).await {
                if value == 2 {
                    continue;
                }
                sum += value;
            }
            sum
        }));
        assert_eq!(poll_ready(fiber.as_mut(), cx.as_mut()), 8);
    });
}

#[test]
fn for_await_runs_to_completion() {
    with_context(|mut cx| {
        let mut fiber = pin!(dope_gen::fiber!('_ => async move {
            let mut sum = 0usize;
            for value in dope_fiber::ready([5usize, 6, 7]).await {
                dope_fiber::ready(()).await;
                if value == 6 {
                    continue;
                }
                sum += value;
                if value == 7 {
                    break;
                }
            }
            sum
        }));
        assert_eq!(poll_ready(fiber.as_mut(), cx.as_mut()), 12);
    });
}

#[test]
fn loop_locals_drop_on_continue_break_and_cancellation() {
    with_context(|mut cx| {
        let drops = Rc::new(Cell::new(0usize));
        let observed = Rc::clone(&drops);
        let mut completed = pin!(dope_gen::fiber!('_ => async move {
            let mut iteration = 0usize;
            loop {
                let _guard = CountDrop(Rc::clone(&drops));
                dope_fiber::ready(()).await;
                iteration += 1;
                if iteration < 4 {
                    continue;
                }
                break;
            }
        }));
        poll_ready(completed.as_mut(), cx.as_mut());
        assert_eq!(observed.get(), 4);

        let drops = Rc::new(Cell::new(0usize));
        let observed = Rc::clone(&drops);
        {
            let mut cancelled = pin!(dope_gen::fiber!('_ => async move {
                loop {
                    let _guard = CountDrop(Rc::clone(&drops));
                    dope_fiber::pending::<()>().await;
                }
            }));
            assert_eq!(Fiber::poll(cancelled.as_mut(), cx.as_mut()), Poll::Pending);
        }
        assert_eq!(observed.get(), 1);
    });
}

#[test]
#[expect(clippy::never_loop)]
fn loop_panic_drops_live_locals_and_poisons_generated_fiber() {
    with_context(|mut cx| {
        let drops = Rc::new(Cell::new(0usize));
        let observed = Rc::clone(&drops);
        let mut fiber = pin!(dope_gen::fiber!('_ => async move {
            loop {
                let _guard = CountDrop(Rc::clone(&drops));
                dope_fiber::ready(()).await;
                ::core::panic!("loop panic");
            }
        }));
        assert_panics_with(
            || {
                let _ = Fiber::poll(fiber.as_mut(), cx.as_mut());
            },
            "loop panic",
        );
        assert_eq!(observed.get(), 1);
        assert_panics_with(
            || {
                let _ = Fiber::poll(fiber.as_mut(), cx.as_mut());
            },
            "after panic",
        );
        assert_eq!(observed.get(), 1);
    });
}

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct App {
    #[pin]
    #[manifold]
    dummy: Dummy,
}

struct Dummy;

impl<'d> Manifold<'d> for Dummy {
    fn pre_park(self: Pin<&mut Self>, _driver: &mut dope::DriverContext<'_, 'd>) {
        let _ = self;
    }
}

struct Ranking;

impl Ranking {
    fn rank<'d>(
        &self,
        user: Vec<u8>,
    ) -> impl dope_fiber::Fiber<'d, Output = Result<Option<usize>, ()>> + use<'d> {
        dope_fiber::ready(Ok(Some(user.len())))
    }
}

fn executor() -> Executor {
    let config = dope::driver::Config::for_profile::<dope::runtime::profile::Throughput>();
    Executor::new(config).expect("executor")
}

#[test]
fn nested_await_runs_to_completion() {
    let value = executor().enter(|mut session| {
        let fiber = dope_gen::fiber!('_ => async move {
            dope_fiber::ready(dope_fiber::ready(7usize).await).await
        });
        session.with_app(App { dummy: Dummy }, |mut app| {
            app.block_on(fiber).expect("runtime park")
        })
    });
    assert_eq!(value, 7);
}

#[test]
fn fully_qualified_known_macro_lowers_await() {
    let matched = executor().enter(|mut session| {
        let fiber = dope_gen::fiber!('_ => async move {
            ::core::matches!(dope_fiber::ready(1usize).await, 1)
        });
        session.with_app(App { dummy: Dummy }, |mut app| {
            app.block_on(fiber).expect("runtime park")
        })
    });
    assert!(matched);
}

#[test]
fn unqualified_standard_macros_are_canonicalized() {
    let (values, formatted, matched) = executor().enter(|mut session| {
        let fiber = dope_gen::fiber!('_ => async move {
            let values = vec![
                dope_fiber::ready(1usize).await,
                dope_fiber::ready(2usize).await,
                3,
            ];
            let formatted = format!("{}", dope_fiber::ready(7usize).await);
            let matched = matches!(dope_fiber::ready(Some(3usize)).await, Some(3));
            (values, formatted, matched)
        });
        session.with_app(App { dummy: Dummy }, |mut app| {
            app.block_on(fiber).expect("runtime park")
        })
    });
    assert_eq!(values, [1, 2, 3]);
    assert_eq!(formatted, "7");
    assert!(matched);
}

#[test]
fn awaited_clone_keeps_owned_local_movable() {
    let (user, rank) = executor().enter(|mut session| {
        let fiber = dope_gen::fiber!('_ => async move {
            let ranking = Ranking;
            let user = String::from("alice").into_bytes();
            match ranking.rank(user.clone()).await {
                Ok(Some(rank)) => (user, rank),
                Ok(None) | Err(()) => (user, 0),
            }
        });
        session.with_app(App { dummy: Dummy }, |mut app| {
            app.block_on(fiber).expect("runtime park")
        })
    });
    assert_eq!(user, b"alice");
    assert_eq!(rank, 5);
}
