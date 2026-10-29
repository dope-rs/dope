use std::mem;

use dope_sys::{
    engine::{Buffer, Complete},
    io::reactor::complete,
    memory::buffer,
    table::permit::Queue as _,
};
use o3::buffer::{
    bytes::bounded,
    storage::{
        provided,
        provided::{Unique, fixed},
    },
};

use crate::{
    Engine, Events, driver,
    driver::{life, route},
    io::transfer,
};

struct Provided;

/// One storage buffer selected by the kernel for this live driver.
#[must_use = "a selected buffer must be delivered or returned"]
#[repr(transparent)]
pub(crate) struct Token<'d, T = <Engine as Buffer>::Token> {
    native: T,
    driver: route::Brand<'d, Provided>,
}

pub(crate) type Entry<'d> =
    Token<'d, <Events as complete::Map>::Entry<<Engine as Complete>::Pending>>;

impl<'d, T> Token<'d, T> {
    pub(crate) fn decode<M: complete::Map<Entry<P> = T>, P>(self) -> M::Entry<Token<'d, P>> {
        M::map(self.native, |native| Token {
            native,
            driver: self.driver,
        })
    }
}

impl<'d> Token<'d, <Engine as Complete>::Pending> {
    pub(crate) fn release(self, local: &mut life::Local<'d>) {
        // SAFETY: Root::enter pairs the sole branded Local with its native loan.
        // Token::next issues from that Local; consuming decode and completion
        // preserve the brand, so this unprojected slot returns to its issuer.
        unsafe { buffer::raw::Release::release(&mut *local.backend, self.native) };
        if let Ok(permit) = crate::Permits::try_acquire() {
            let driver = local.driver;
            super::Proof::release(&permit, driver.receive().exhausted(), driver);
        }
    }
}

impl<'d, T: Into<<Engine as Buffer>::Token>> Token<'d, T> {
    pub(crate) fn bind(self, vacant: Vacant<'d>) -> bounded::Value<Unique<'d>> {
        use o3::buffer;

        // SAFETY: Token and Vacant retain the same driver brand and native context.
        // The completed token uniquely owns its initialized prefix, whose length
        // was admitted against the unchanged receive layout before this binding.
        let buffer = unsafe { buffer::raw::Fixed::insert(vacant.native, self.native.into()) };
        // SAFETY: the completed native owner retains its ABI-bounded prefix.
        let proof = unsafe { bounded::raw::Input::new(buffer) };
        bounded::Value::from_raw(proof)
    }
}

const _: () = {
    assert!(mem::size_of::<Token<'static>>() == mem::size_of::<<Engine as Buffer>::Token>());
    assert!(mem::align_of::<Token<'static>>() == mem::align_of::<<Engine as Buffer>::Token>());
    assert!(
        mem::size_of::<Option<Token<'static>>>()
            == mem::size_of::<Option<<Engine as Buffer>::Token>>()
    );
    assert!(mem::size_of::<Vacant<'static>>() == 2 * mem::size_of::<usize>());
};

impl<'d, T: buffer::raw::Pending<Extent = transfer::Extent>> buffer::raw::Pending for Token<'d, T> {
    type Extent = transfer::Extent;
    type Complete = Token<'d, T::Complete>;

    fn try_complete(
        self,
        len: transfer::Len,
    ) -> Result<Self::Complete, (Self, transfer::Len, transfer::NonZero)> {
        match self.native.try_complete(len) {
            Ok(native) => Ok(Token {
                native,
                driver: self.driver,
            }),
            Err((native, actual, capacity)) => Err((
                Self {
                    native,
                    driver: self.driver,
                },
                actual,
                capacity,
            )),
        }
    }
}

impl<T: dope_sys::Buffer> Token<'_, T> {
    pub(crate) fn len(&self) -> usize {
        self.native.len()
    }
}

/// The fixed vacancy already retains its source arena's invariant lifetime.
#[repr(transparent)]
pub(crate) struct Vacant<'d> {
    native: provided::Vacant<'d, <Engine as Buffer>::Token, (), &'d driver::Shared>,
}

impl<'d> Vacant<'d> {
    pub(crate) fn try_new(driver: driver::Reference<'d>) -> Result<Self, fixed::Full> {
        Ok(Self {
            native: driver.receive().owners().try_vacant()?,
        })
    }
}

impl<'d> Token<'d> {
    pub(crate) fn next(local: &mut life::Local<'d>) -> Option<Option<Entry<'d>>> {
        let native = Complete::next(&mut *local.backend)?;
        Some(native.map(|native| Token {
            native,
            driver: route::Brand::new(local.driver.brand),
        }))
    }
}
