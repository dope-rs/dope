pub fn cases() -> impl Iterator<Item = (String, &'static [&'static str])> {
    let app = concat!(
        "use core::{marker::Copy as FnMut, unreachable as assert};\n",
        "use dope_gen::Application as Copy;\n",
        "const __app: usize = 0;\n",
        "const __field: usize = 0;\n",
        "const context: usize = 0;\n",
        "const driver: usize = 0;\n",
        "const turn: usize = 0;\n",
        "const shared: usize = 0;\n",
        "const exclusive: usize = 0;\n",
        include_str!("compile_application.rs"),
    );
    let unpinned = concat!(
        "use core::{compile_error as stringify, stringify as compile_error};\n",
        include_str!("compile_application_unpinned.rs"),
    );
    let coord = r#"
use engine::{core::driver::route::KeyTag, manifold::timing::interval::Interval};
#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(crate = ::engine)]
#[coordinate]
struct App<'d> {
    #[pin]
    #[manifold]
    step: Interval<'d, KeyTag<1>>,
}
impl<'d> App<'d> {
    fn coordinate(_: app_coord::Fields<'_, 'd>) -> engine::runtime::control::Flow {
        engine::runtime::control::Flow::Idle
    }
}
fn main() {}
"#;
    let defaulted_coord = coord
        .replace("struct App<'d>", "struct App<'d, const N: u8 = 1>")
        .replace("KeyTag<1>", "KeyTag<N>")
        .replace("impl<'d> App<'d>", "impl<'d, const N: u8> App<'d, N>")
        .replace("app_coord::Fields<'_, 'd>", "app_coord::Fields<'_, 'd, N>");
    let relative_header = defaulted_coord
        .replace("const N: u8", "const N: self::Width")
        .replace("fn main() {}", "type Width = u8;\nfn main() {}");
    let sibling = coord
        .replace(
            "fn main() {}",
            "struct __AppFields;\nstruct Fields;\nfn main() {}",
        )
        .replace(
            "    step: Interval<'d, KeyTag<1>>,",
            "    step: Interval<'d, KeyTag<1>>,\n    #[dispatcher(state)]\n    fields: (Fields, self::Fields),",
        );
    let hidden = coord.replace("app_coord::Fields", "__AppFields");
    let routes = coord.replace(
        "    step: Interval<'d, KeyTag<1>>,",
        "    foo: Interval<'d, KeyTag<1>>,\n    #[pin]\n    #[manifold]\n    r#FOO: Interval<'d, KeyTag<2>>,",
    );
    let unicode_routes = routes.replace("foo:", "ß:").replace("r#FOO:", "ss:");
    let owner = coord
        .replace("#[manifold]", "#[manifold(control)]")
        .replace("step:", "r#step:");
    let state = r#"
#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(crate = ::engine)]
struct App {
    #[dispatcher(state)]
    step: usize,
}
fn main() {}
"#;
    let reserved = state.replace("struct App", "#[coordinate]\nstruct App");
    let safe_drain =
        format!("{state}\nimpl<'d> engine::runtime::executor::raw::Drain<'d> for App {{}}\n");
    let generic = r#"
#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(crate = ::engine)]
struct App<const __app: usize> {
    #[dispatcher(state)]
    bytes: [u8; __app],
}
fn main() {}
"#;
    let shadows = r#"
#[derive(dope_gen::WorkGraph)]
#[work(core = ::engine::core, runtime = ::engine::runtime)]
enum bool { First, Last }
#[derive(dope_gen::WorkGraph)]
#[work(core = ::engine::core, runtime = ::engine::runtime)]
enum usize { First, Last }
struct __WorkCycle;
use Work::{done, slot, ready, admit};
#[derive(dope_gen::WorkGraph)]
#[work(core = ::engine::core, runtime = ::engine::runtime)]
enum r#Work { done, slot, ready, admit }
fn main() {
    const done: ::core::primitive::usize = 0;
    const slot: ::core::primitive::usize = 0;
    const ready: ::core::primitive::usize = 0;
    #[derive(dope_gen::WorkGraph)]
    #[work(core = ::engine::core, runtime = ::engine::runtime)]
    enum Local { First, Last }
    let _ = LocalGraph::new();
}
"#;
    let prim = concat!("struct usize;\n", include_str!("compile_application.rs"));
    let names = r#"
#[dope_gen::fiber_fn('d, crate = ::fibers)]
async fn retained<
    'd,
    const __dope_brand: usize,
    const __dope_seal: usize,
    const __dope_future: usize,
    const __dope_arg0: usize,
    const __dope_fiber: usize,
>(value: usize) -> usize {
    fibers::abi::Ready::new(value).await
}
fn main() {
    fn require(_: impl fibers::abi::Fiber<'static, Output = usize>) {}
    require(retained::<0, 0, 0, 0, 0>(7));
}
"#;
    let outer = r#"
fn retained<
    const __dope_brand: usize,
    const __dope_seal: usize,
    const __dope_future: usize,
    const __dope_fiber: usize,
>() -> impl fibers::abi::Fiber<'static, Output = usize> {
    dope_gen::fiber!('static, crate = ::fibers => async move {
        fibers::abi::Ready::new(7).await
    })
}
fn main() {
    let _ = retained::<0, 0, 0, 0>();
}
"#;
    let unsafe_body = outer.replace(
        "fibers::abi::Ready::new(7).await",
        "core::ptr::read(&7usize)",
    );
    let arguments = r#"
struct Owner<const __dope_arg0: usize, const __dope_arg1: usize>;
impl<const __dope_arg0: usize, const __dope_arg1: usize> Owner<__dope_arg0, __dope_arg1> {
    #[dope_gen::fiber_fn('d, crate = ::fibers)]
    async fn retained<'d>(value: usize, tail: usize) -> usize {
        fibers::abi::Ready::new(value + tail).await
    }
}
fn main() {
    fn require(_: impl fibers::abi::Fiber<'static, Output = usize>) {}
    require(Owner::<0, 0>::retained(3, 4));
}
"#;
    let unnamed = arguments
        .replace("value: usize", "_: usize")
        .replace("value + tail", "3 + tail");
    let item = arguments
        .replace("struct Owner", "struct Token;\nstruct Owner")
        .replace("value: usize", "Token: Token")
        .replace("value + tail", "tail")
        .replace("retained(3, 4)", "retained(Token, 4)");
    let bound = item.replace("Token: Token", "_token @ Token: Token");
    let escaped = r#"
fn main() {
    let _ = dope_gen::fiber!('_, crate = ::fibers => async move {
        async { () }
    });
}
"#;
    let moved = escaped.replace("async { () }", "(async move { () }).await");
    let attrs = [
        r#"#[cfg_attr(any(), opaque::rewrite)] { fibers::abi::Ready::new(()).await; }"#,
        r#"#[cfg_attr(all(), cfg_attr(any(), opaque::rewrite))] { fibers::abi::Ready::new(()).await; }"#,
        r#"let local = #[cfg_attr(any(), opaque::rewrite)] || (); local();"#,
        r#"#[cfg_attr(any(), opaque::rewrite)] fn local() {} local();"#,
        r#"struct Local; impl Local { #[cfg_attr(any(), opaque::rewrite)] fn local() {} } Local::local();"#,
        r#"trait Local { #[cfg_attr(any(), opaque::rewrite)] fn local() {} }"#,
        r#"#[cfg_attr(any(), opaque::rewrite)] tracing::info!(7);"#,
        r#"let _ = #[cfg_attr(any(), opaque::rewrite)] tracing::info!(7);"#,
    ]
    .map(|body| {
        format!(
            r#"mod tracing {{ pub use core::stringify as info; }}
fn main() {{
    let _ = dope_gen::fiber!('_, crate = ::fibers => async move {{ {body} }});
}}
"#
        )
    });
    let inert = r#"
#[dope_gen::fiber_fn('d, crate = ::fibers)]
#[deny(unused_variables)]
#[inline]
#[cold]
#[must_use]
#[deprecated]
#[track_caller]
async fn retained<'d>() {
    #[cfg_attr(all(), allow(unused_variables))]
    { let unused = 7; fibers::abi::Ready::new(()).await; }
}
fn main() { let _ = retained(); }
"#;
    let tagged = inert.replace("#[deny(unused_variables)]", "#[opaque::rewrite]");
    let cases: [(String, &[&str]); 30] = [
        (include_str!("compile_fiber.rs").into(), &[]),
        (
            include_str!("compile_future.rs").into(),
            &["error[E0277]:", "std::future::Ready<()>", "Fiber<'_>"],
        ),
        (app.into(), &[]),
        (unpinned.into(), &["error: DopeAppPinRequired"]),
        (
            include_str!("compile_borrowed.rs").into(),
            &["error: DopeAppUnknownOwner"],
        ),
        (coord.into(), &[]),
        (defaulted_coord, &[]),
        (relative_header, &["error: DopeAppCoordinateHeader"]),
        (sibling, &[]),
        (hidden, &["cannot find type `__AppFields`"]),
        (routes, &["error: DopeAppDuplicateRouteName"]),
        (unicode_routes, &["error: DopeAppDuplicateRouteName"]),
        (owner, &["error: DopeAppCoordinateStep"]),
        (state.into(), &[]),
        (reserved, &["error: DopeAppCoordinateStep"]),
        (safe_drain, &["error[E0200]:"]),
        (generic.into(), &[]),
        (shadows.into(), &[]),
        (prim.into(), &[]),
        (names.into(), &[]),
        (outer.into(), &[]),
        (unsafe_body, &["error[E0133]:"]),
        (arguments.into(), &[]),
        (unnamed, &["error: DopeFiberArgumentCapture"]),
        (item, &["error[E0530]:"]),
        (bound, &[]),
        (escaped.into(), &["error: DopeFiberAsyncScope"]),
        (moved, &["error: DopeFiberAsyncScope"]),
        (inert.into(), &[]),
        (tagged, &["error: DopeFiberAttribute"]),
    ];
    let invalid = [
        "",
        "crate ::engine",
        "other = ::engine",
        "crate =",
        "crate = ::engine, other = ::engine",
        "crate = ::engine,,",
    ]
    .map(|options| app.replace("forward(crate = ::engine,)", &format!("forward({options})")));
    cases
        .into_iter()
        .chain(
            attrs
                .into_iter()
                .map(|source| (source, &["error: DopeFiberAttribute"][..])),
        )
        .chain(
            invalid
                .into_iter()
                .map(|source| (source, &["error: DopeManifoldCrate"][..])),
        )
}
