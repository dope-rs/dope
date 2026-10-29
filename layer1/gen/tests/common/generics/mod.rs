use std::marker::PhantomData;

use engine::{
    core::driver::route::{Identity, KeyTag},
    runtime::{control, executor::raw::Drain},
};

use crate::common::macros::counter::Counter;

pub mod paths;

pub trait Loan<'a> {}

impl<'a> Loan<'a> for () {}

type Borrows<'a, 'b, 'c, T> = (&'a (), &'b (), &'c (), T);

#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine)]
pub struct GenericOwner<T> {
    #[pin]
    #[manifold]
    pub inner: T,
}

#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine)]
pub struct DefaultApp<'d, ID: Identity = KeyTag<9>, const N: usize = 1> {
    #[pin]
    #[manifold]
    pub r#type: Counter<ID, (&'d (), [(); N])>,
}

#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine)]
pub struct DefaultDrain<'d, T, const N: usize = 1>
where
    T: Drain<'d>,
{
    #[pin]
    #[app]
    #[drain]
    pub inner: T,
    #[dispatcher(state)]
    pub marker: PhantomData<(&'d (), [(); N])>,
}

#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine, driver = 'd)]
pub struct Names<
    'd,
    '__app,
    '__driver,
    '__turn,
    '__root,
    '__owner,
    '__finish,
    '__epoch,
    '__coordinate_step,
    T = (),
> where
    T: for<'__app_> Loan<'__app_>,
{
    #[pin]
    #[manifold]
    pub inner: Counter<
        KeyTag<12>,
        Borrows<
            'd,
            '__app,
            '__driver,
            Borrows<
                '__turn,
                '__root,
                '__owner,
                Borrows<'__finish, '__epoch, '__coordinate_step, T>,
            >,
        >,
    >,
}

#[pin_project::pin_project]
#[derive(engine::Forward)]
#[forward(crate = ::engine)]
pub struct ForwardNames<'d, '__owner, '__turn, 'r#__root> {
    #[pin]
    #[forward('d)]
    pub inner: Counter<KeyTag<13>, Borrows<'d, '__owner, '__turn, &'r#__root ()>>,
}

#[pin_project::pin_project]
#[derive(engine::Application, engine::Forward)]
#[dispatcher(crate = ::engine)]
#[forward(crate = ::engine)]
pub struct BoundNames<T = ()>
where
    T: for<'__d> Loan<'__d>,
{
    #[pin]
    #[manifold]
    #[forward]
    pub inner: Counter<KeyTag<14>, T>,
}

pub trait Marker<T: ?Sized> {}

impl<T: ?Sized> Marker<T> for () {}

#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine)]
pub struct SelfBounds<'d, T = ()>
where
    T: Marker<Self>,
{
    #[pin]
    #[manifold]
    pub inner: Counter<KeyTag<15>, (&'d (), T)>,
}

pub trait Borrowed {
    type Value<'a>;
}

#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine)]
#[coordinate]
pub struct SelfFields<'__coordinate_step> {
    #[dispatcher(state)]
    pub root: Option<Box<Self>>,
    #[dispatcher(state)]
    pub held: <Self as Borrowed>::Value<'__coordinate_step>,
}

impl Borrowed for SelfFields<'_> {
    type Value<'a> = &'a str;
}

impl SelfFields<'_> {
    fn coordinate(_: self_fields_coord::Fields<'_, '_>) -> control::Flow {
        control::Flow::Idle
    }
}

#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine)]
#[coordinate]
pub struct SelfConst {
    #[dispatcher(state)]
    pub bytes: [u8; {
        struct Local<'a>(Option<&'a Self>);
        let _ = Local(Some(&Local(None))).0;
        Self::LEN
    }],
}

impl SelfConst {
    pub const LEN: usize = 1;

    fn coordinate(_: self_const_coord::Fields<'_, '_>) -> control::Flow {
        control::Flow::Idle
    }
}
