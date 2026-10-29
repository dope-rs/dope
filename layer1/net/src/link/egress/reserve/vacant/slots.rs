use o3::base::cell::region;

use crate::link::egress::{
    NonEmpty,
    load::{Value, credit},
    metadata::pool,
    reserve::{
        Bounds, Publish, demand,
        error::{self, Error},
        guard::Rollback,
        limit,
    },
};

#[must_use = "reserved slots must be committed or rolled back"]
pub(super) enum Slots<'queue, 'p, 'd, B, D: demand::Mode> {
    Empty,
    First(pool::Vacant<'queue, 'p, 'd, B, (), credit::Ready<'p, 'd>, D>),
    Second(pool::Vacant<'queue, 'p, 'd, B, (), credit::Ready<'p, 'd>, D>),
    Pair(
        pool::Vacant<'queue, 'p, 'd, B, (), credit::Ready<'p, 'd>, D>,
        pool::Vacant<'queue, 'p, 'd, B, (), credit::Ready<'p, 'd>, D>,
    ),
}

struct Inputs<B> {
    first: Option<Value<B>>,
    second: Option<Value<B>>,
}

struct Validated<B> {
    first: Option<NonEmpty<B>>,
    second: Option<NonEmpty<B>>,
}

enum Admitted<'queue, 'p, 'd, B, D: demand::Mode> {
    Empty(Slots<'queue, 'p, 'd, B, D>),
    One {
        commit: pool::Bound<'queue, 'p, 'd, B, D>,
        spare: Option<pool::Vacant<'queue, 'p, 'd, B, (), credit::Ready<'p, 'd>, D>>,
    },
    Pair(
        pool::Bound<'queue, 'p, 'd, B, D>,
        pool::Bound<'queue, 'p, 'd, B, D>,
    ),
}

impl<B> Inputs<B> {
    fn new(bounds: Bounds, first: Option<Value<B>>, second: Option<Value<B>>) -> Self {
        if first.is_none() && second.is_some() && !bounds.has_second() {
            Self {
                first: second,
                second: None,
            }
        } else {
            Self { first, second }
        }
    }

    fn validate(self, bounds: Bounds) -> Result<Validated<B>, Error<B>> {
        let check = |input| match input {
            None => Ok(None),
            Some(Value::Checked(load)) => Ok(Some(load)),
            Some(Value::Unchecked(load)) => load.try_nonempty().map(Some),
        };
        let Self { first, second } = self;
        let first = match check(first) {
            Ok(first) => first,
            Err(error) => {
                let (load, reason) = error.into_parts();
                return Err(Error::invalid(
                    error::Part::First,
                    reason,
                    bounds,
                    Some(Value::Unchecked(load)),
                    second,
                ));
            }
        };
        let second = match check(second) {
            Ok(second) => second,
            Err(error) => {
                let (load, reason) = error.into_parts();
                return Err(Error::invalid(
                    error::Part::Second,
                    reason,
                    bounds,
                    first.map(Value::Checked),
                    Some(Value::Unchecked(load)),
                ));
            }
        };
        Ok(Validated { first, second })
    }
}

impl<'queue, 'p, 'd, B, D: demand::Mode> Slots<'queue, 'p, 'd, B, D> {
    pub(super) fn try_publish(
        self,
        bounds: Bounds,
        first: Option<Value<B>>,
        second: Option<Value<B>>,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> Result<Publish, Error<B>> {
        let valid = match Inputs::new(bounds, first, second).validate(bounds) {
            Ok(valid) => valid,
            Err(error) => {
                self.rollback(token);
                return Err(error);
            }
        };
        let admitted = self.admit(bounds, valid, token)?;
        Ok(admitted.publish(token))
    }

    fn admit(
        self,
        bounds: Bounds,
        valid: Validated<B>,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> Result<Admitted<'queue, 'p, 'd, B, D>, Error<B>> {
        let Validated { first, second } = valid;
        match self {
            Self::Empty => match (first, second) {
                (None, None) => Ok(Admitted::Empty(Self::Empty)),
                (Some(first), second) => Self::reject(
                    Self::Empty,
                    error::Mismatch::First(limit::Mismatch::Empty {
                        actual: first.geometry().wire(),
                    }),
                    bounds,
                    Some(Value::Checked(first)),
                    second.map(Value::Checked),
                    token,
                ),
                (None, Some(second)) => Self::reject(
                    Self::Empty,
                    error::Mismatch::UnexpectedSecond {
                        wire: second.geometry().wire(),
                        resident: second.geometry().resident(),
                    },
                    bounds,
                    None,
                    Some(Value::Checked(second)),
                    token,
                ),
            },
            Self::First(node) => match (first, second) {
                (None, None) => Ok(Admitted::Empty(Self::First(node))),
                (Some(first), None) => match bind(node, first, bounds.first()) {
                    Ok(commit) => Ok(Admitted::One {
                        commit,
                        spare: None,
                    }),
                    Err((node, first, cause)) => Self::reject(
                        Self::First(node),
                        error::Mismatch::First(cause),
                        bounds,
                        Some(Value::Checked(first)),
                        None,
                        token,
                    ),
                },
                (first, Some(second)) => Self::reject(
                    Self::First(node),
                    error::Mismatch::UnexpectedSecond {
                        wire: second.geometry().wire(),
                        resident: second.geometry().resident(),
                    },
                    bounds,
                    first.map(Value::Checked),
                    Some(Value::Checked(second)),
                    token,
                ),
            },
            Self::Second(node) => match (first, second) {
                (None, None) => Ok(Admitted::Empty(Self::Second(node))),
                (None, Some(second)) => match bind(node, second, bounds.second()) {
                    Ok(commit) => Ok(Admitted::One {
                        commit,
                        spare: None,
                    }),
                    Err((node, second, cause)) => Self::reject(
                        Self::Second(node),
                        error::Mismatch::Second(cause),
                        bounds,
                        None,
                        Some(Value::Checked(second)),
                        token,
                    ),
                },
                (Some(first), second) => Self::reject(
                    Self::Second(node),
                    error::Mismatch::First(limit::Mismatch::Empty {
                        actual: first.geometry().wire(),
                    }),
                    bounds,
                    Some(Value::Checked(first)),
                    second.map(Value::Checked),
                    token,
                ),
            },
            Self::Pair(left, right) => match (first, second) {
                (None, None) => Ok(Admitted::Empty(Self::Pair(left, right))),
                (Some(first), None) => match bind(left, first, bounds.first()) {
                    Ok(commit) => Ok(Admitted::One {
                        commit,
                        spare: Some(right),
                    }),
                    Err((left, first, cause)) => Self::reject(
                        Self::Pair(left, right),
                        error::Mismatch::First(cause),
                        bounds,
                        Some(Value::Checked(first)),
                        None,
                        token,
                    ),
                },
                (None, Some(second)) => match bind(right, second, bounds.second()) {
                    Ok(commit) => Ok(Admitted::One {
                        commit,
                        spare: Some(left),
                    }),
                    Err((right, second, cause)) => Self::reject(
                        Self::Pair(left, right),
                        error::Mismatch::Second(cause),
                        bounds,
                        None,
                        Some(Value::Checked(second)),
                        token,
                    ),
                },
                (Some(first), Some(second)) => {
                    let a = match bind(left, first, bounds.first()) {
                        Ok(part) => part,
                        Err((left, first, cause)) => {
                            return Self::reject(
                                Self::Pair(left, right),
                                error::Mismatch::First(cause),
                                bounds,
                                Some(Value::Checked(first)),
                                Some(Value::Checked(second)),
                                token,
                            );
                        }
                    };
                    let b = match bind(right, second, bounds.second()) {
                        Ok(part) => part,
                        Err((right, second, cause)) => {
                            let (left, first) = a.into_parts();
                            return Self::reject(
                                Self::Pair(left, right),
                                error::Mismatch::Second(cause),
                                bounds,
                                Some(Value::Checked(first)),
                                Some(Value::Checked(second)),
                                token,
                            );
                        }
                    };
                    Ok(Admitted::Pair(a, b))
                }
            },
        }
    }

    fn reject(
        self,
        cause: error::Mismatch,
        bounds: Bounds,
        first: Option<Value<B>>,
        second: Option<Value<B>>,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> Result<Admitted<'queue, 'p, 'd, B, D>, Error<B>> {
        let error = Error::changed(cause, bounds, first, second);
        self.rollback(token);
        Err(error)
    }
}

fn bind<'queue, 'p, 'd, B, D: demand::Mode>(
    node: pool::Vacant<'queue, 'p, 'd, B, (), credit::Ready<'p, 'd>, D>,
    load: NonEmpty<B>,
    limit: limit::Limit,
) -> Result<pool::Bound<'queue, 'p, 'd, B, D>, BindFailure<'queue, 'p, 'd, B, D>> {
    if let Err(cause) = limit.check(load.geometry().wire()) {
        return Err((node, load, cause));
    }
    node.try_bind(load)
        .map_err(|(node, load, cause)| (node, load, limit::Mismatch::Credit(cause)))
}

impl<'queue, 'p, 'd, B, D: demand::Mode> Admitted<'queue, 'p, 'd, B, D> {
    fn publish(self, token: &mut region::Token<region::Identity<'d>>) -> Publish {
        match self {
            Self::Empty(slots) => {
                slots.rollback(token);
                Publish::Empty
            }
            Self::One { commit, spare } => {
                if let Some(node) = spare {
                    node.rollback(token);
                }
                commit.publish(token);
                Publish::Published
            }
            Self::Pair(left, right) => {
                left.publish(token);
                right.publish(token);
                Publish::Published
            }
        }
    }
}

impl<'p, 'd, B, D: demand::Mode> Rollback<'d> for Slots<'_, 'p, 'd, B, D> {
    fn rollback(self, token: &mut region::Token<region::Identity<'d>>) {
        match self {
            Self::Empty => {}
            Self::First(node) | Self::Second(node) => {
                node.rollback(token);
            }
            Self::Pair(first, second) => {
                second.rollback(token);
                first.rollback(token);
            }
        }
    }
}

type BindFailure<'queue, 'p, 'd, B, D> = (
    pool::Vacant<'queue, 'p, 'd, B, (), credit::Ready<'p, 'd>, D>,
    NonEmpty<B>,
    limit::Mismatch,
);
