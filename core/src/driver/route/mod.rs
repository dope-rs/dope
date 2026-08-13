mod bound;
mod operation;
mod sealed;
mod space;
pub mod table;
mod target;
use core::{marker, mem};

pub use bound::Bound;
pub use operation::Operation;
pub(crate) use sealed::Private;
pub use sealed::{Epoch, SlotIndex, Token};
pub use space::Space;
pub use target::Target;

type Brand<'d, Tag> = marker::PhantomData<(fn(&'d ()) -> &'d (), *mut Tag)>;

pub const FRAMEWORK: u8 = 255;
pub(crate) const CAPACITY: usize = u8::MAX as usize + 1;

pub(crate) const SHIFT: u32 = 56;
pub(crate) const KIND_SHIFT: u32 = 48;
pub const SLOT_BITS: u32 = 24;
pub const SLOT_MASK: u64 = (1 << SLOT_BITS) - 1;
/// Maximum logical generation. Zero remains reserved for framework controls.
pub const EPOCH_MASK: u64 = u64::MAX - 1;

#[derive(Clone, Copy)]
pub struct KeyTag<const ROUTE: u8, const KIND: u8 = 0>;

pub trait Tag: Copy {
    const ROUTE: u8;
    const KIND: u8;
}

impl<const ROUTE: u8, const KIND: u8> Tag for KeyTag<ROUTE, KIND> {
    const ROUTE: u8 = ROUTE;
    const KIND: u8 = KIND;
}

/// Closed driver-branded identities accepted by receive-credit state.
#[doc(hidden)]
pub trait Credit<'d>: Private + Copy {
    #[doc(hidden)]
    fn into_credit_token(self) -> Token;
}

/// Operation identity after deliberately erasing only its route tag.
#[doc(hidden)]
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Erased<'d> {
    token: Token,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d> Erased<'d> {
    pub(in crate::driver) const fn new(token: Token) -> Self {
        Self {
            token,
            driver: marker::PhantomData,
        }
    }

    pub(crate) fn matches(self, token: Token) -> bool {
        self.token == token
    }
}

impl Private for Erased<'_> {}

impl<'d> Credit<'d> for Erased<'d> {
    fn into_credit_token(self) -> Token {
        self.token
    }
}

const _: () = assert!(mem::size_of::<Erased<'static>>() == mem::size_of::<Token>());

pub mod kind;
pub const RECV: u8 = kind::RECV;
pub const SEND: u8 = kind::SEND;

pub const SHUTDOWN: Token = Token::framework(SlotIndex::ZERO).with_kind(kind::SHUTDOWN);
