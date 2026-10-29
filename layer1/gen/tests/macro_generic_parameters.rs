pub mod common;

use common::{generics::*, macros::counter::Counter};
use engine::{
    core::driver::route::{Id, KeyTag},
    manifold::app::dispatch::raw::Manifold,
    runtime::executor,
};

#[test]
fn generic_contracts() {
    fn application<'d>(_: &impl executor::raw::App<'d>) {}
    fn route<'d, T: Manifold<'d>>() -> Id {
        GenericOwner::<T>::INNER_ROUTE
    }
    assert!(route::<Counter<KeyTag<14>>>() == Id::fixed::<14>());
    let app: DefaultApp<'_> = DefaultApp {
        r#type: Counter::default(),
    };
    application(&app);
    assert!(DefaultApp::<'static, KeyTag<9>, 1>::TYPE_ROUTE == Id::fixed::<9>());

    application(&Names::<()> {
        inner: Counter::default(),
    });
    application(&BoundNames::<()> {
        inner: Counter::default(),
    });
    application(&SelfBounds::<()> {
        inner: Counter::default(),
    });
    let text = String::from("borrowed");
    application(&SelfFields {
        root: None,
        held: &text,
    });
    application(&SelfConst {
        bytes: [0; SelfConst::LEN],
    });
    application(&paths::Relative {
        outer: SelfConst {
            bytes: [0; SelfConst::LEN],
        },
    });
    fn manifold<'d>(_: &impl Manifold<'d>) {}
    manifold(&ForwardNames {
        inner: Counter::default(),
    });
    manifold(&BoundNames::<()> {
        inner: Counter::default(),
    });
}
