//! Runtime client integration coverage.

use std::{hint, mem, pin::Pin, task::Poll};

use dope_runtime::{
    client::{Anchor, Composition, Lease, Provider, Scope},
    executor,
};

#[derive(dope_gen::Application)]
#[dispatcher(core = ::dope_core, runtime = ::dope_runtime)]
struct App {}

struct Client<'app, 'd: 'app> {
    _lease: Lease<'app, 'd>,
}

impl<'d> Provider<'d> for App {
    type Client<'app>
        = Client<'app, 'd>
    where
        'd: 'app;

    fn provide<'app>(self: Pin<&Self>, scope: Scope<'app, 'd, Self>) -> Self::Client<'app>
    where
        'd: 'app,
    {
        let _ = self;
        Client {
            _lease: scope.lease(),
        }
    }
}

struct CheckComposition;

impl<'scope, 'd: 'scope, S, Q> Composition<'scope, 'd, S, Q, App, App> for CheckComposition {
    type Output = (usize, usize);

    fn compose<'app>(
        self,
        client: Client<'app, 'd>,
        root: Anchor<'app, App>,
        _session: &mut executor::session::Session<'scope, 'd, S, Q>,
    ) -> Self::Output
    where
        'd: 'app,
    {
        (mem::size_of_val(&client), mem::size_of_val(&root))
    }
}

struct Immediate<T>(Option<T>);

const IMMEDIATE_DRIVE_ITERATIONS: usize = 4_096;

impl<T: Unpin> Immediate<T> {
    fn take_output(self: Pin<&mut Self>) -> Option<T> {
        self.get_mut().0.take()
    }
}

impl<'d, T: Unpin> executor::Root<'d> for Immediate<T> {
    type Output = T;

    fn poll(context: executor::RootContext<'_, 'd, Self>) -> Poll<Self::Output> {
        let (root, _, _, _) = context.into_parts();
        match Self::take_output(root) {
            Some(output) => Poll::Ready(output),
            None => Poll::Pending,
        }
    }
}

fn immediate_drive_case(extra_iterations: usize) -> (usize, usize) {
    dope_test::scenario::rt::Runtime::throughput()
        .executor()
        .enter(|mut session| {
            session
                .with_app(App {}, |mut app| {
                    hint::black_box(
                        app.drive(Immediate(Some(0usize)))
                            .expect("warm immediate drive"),
                    );

                    dope_test::checks::TrackingAlloc::<0>::during(|| {
                        for output in 0..extra_iterations {
                            hint::black_box(
                                app.drive(Immediate(Some(output))).expect("immediate drive"),
                            );
                        }
                    })
                })
                .expect("application teardown")
        })
}

#[test]
fn immediate_drive_syscall_baseline() {
    assert_eq!(immediate_drive_case(0), (0, 0));
}

#[test]
fn immediate_drive_syscall_probe_allocates_nothing() {
    assert_eq!(immediate_drive_case(IMMEDIATE_DRIVE_ITERATIONS), (0, 0));
}

#[test]
fn issued_client_does_not_retain_the_dispatcher_borrow() {
    dope_test::scenario::rt::Runtime::throughput()
        .executor()
        .enter(|mut session| {
            session
                .with_app(App {}, |mut app| {
                    let client = app.client(|app| app);
                    assert_eq!(mem::size_of_val(&client), 0);
                    assert_eq!(app.drive(Immediate(Some(7))).unwrap(), 7,);
                    assert_eq!(app.drive(Immediate(Some(11))).unwrap(), 11,);
                    let _ = client;
                })
                .expect("application teardown");
        });
}

#[test]
fn provider_owner_and_client_share_one_non_escaping_scope() {
    dope_test::scenario::rt::Runtime::throughput()
        .executor()
        .enter(|mut session| {
            let sizes = session.with_provider(App {}, |provider| provider, CheckComposition);
            assert_eq!(sizes, (0, mem::size_of::<usize>()));
        });
}

#[test]
fn scope_is_zero_sized() {
    assert_eq!(mem::size_of::<Scope<'static, 'static, App>>(), 0);
    assert_eq!(mem::size_of::<Lease<'static, 'static>>(), 0);
}

#[test]
fn scope_is_copy_without_provider_bounds() {
    fn assert_copy<T: Copy>() {}
    assert_copy::<Scope<'static, 'static, dyn Send>>();
    assert_copy::<Lease<'static, 'static>>();
}

#[test]
fn scope_can_only_be_narrowed_to_its_borrow() {
    fn narrow<'short, 'app: 'short, 'd: 'app, P: ?Sized>(
        scope: &'short Scope<'app, 'd, P>,
    ) -> Scope<'short, 'd, P> {
        scope.reborrow()
    }

    let _ = narrow::<dyn Send>;
}
