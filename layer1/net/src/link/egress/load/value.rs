use crate::link::egress::{self, data::compose};

/// A payload retaining the result of its first geometry check.
pub enum Value<B> {
    Unchecked(egress::Load<B>),
    Checked(egress::NonEmpty<B>),
}

impl<B> Value<B> {
    pub fn charge(&self) -> egress::Charge {
        match self {
            Self::Unchecked(load) => load.charge(),
            Self::Checked(load) => load.charge(),
        }
    }

    pub fn as_inner(&self) -> &B {
        match self {
            Self::Unchecked(load) => load.as_inner(),
            Self::Checked(load) => load.as_inner(),
        }
    }

    pub fn into_inner(self) -> B {
        match self {
            Self::Unchecked(load) => load.into_inner(),
            Self::Checked(load) => load.into_inner(),
        }
    }

    pub fn into_right<L>(self) -> Value<compose::Either<L, B>> {
        match self {
            Self::Unchecked(load) => Value::Unchecked(load.into_right()),
            Self::Checked(load) => Value::Checked(load.into_right()),
        }
    }
}

impl<L, R> Value<compose::Either<L, R>> {
    pub fn split(self) -> compose::Either<Value<L>, Value<R>> {
        match self {
            Self::Unchecked(load) => match load.split() {
                compose::Either::Left(load) => compose::Either::Left(Value::Unchecked(load)),
                compose::Either::Right(load) => compose::Either::Right(Value::Unchecked(load)),
            },
            Self::Checked(load) => match load.split() {
                compose::Either::Left(load) => compose::Either::Left(Value::Checked(load)),
                compose::Either::Right(load) => compose::Either::Right(Value::Checked(load)),
            },
        }
    }
}

const _: () = {
    assert!(size_of::<Value<()>>() == 3 * size_of::<usize>());
    assert!(size_of::<Option<Value<()>>>() == size_of::<Value<()>>());
};
