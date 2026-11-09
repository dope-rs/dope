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

fn validate<B>(
    bounds: Bounds,
    first: Option<Value<B>>,
    second: Option<Value<B>>,
) -> Result<(Option<NonEmpty<B>>, Option<NonEmpty<B>>), Error<B>> {
    let (first, second) = if first.is_none() && second.is_some() && !bounds.has_second() {
        (second, None)
    } else {
        (first, second)
    };
    let check = |input| match input {
        None => Ok(None),
        Some(Value::Checked(load)) => Ok(Some(load)),
        Some(Value::Unchecked(load)) => load.try_nonempty().map(Some),
    };
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
    Ok((first, second))
}

impl<'queue, 'p, 'd, B, D: demand::Mode> Slots<'queue, 'p, 'd, B, D> {
    pub(super) fn try_publish(
        self,
        bounds: Bounds,
        first: Option<Value<B>>,
        second: Option<Value<B>>,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> Result<Publish, Error<B>> {
        let (first, second) = match validate(bounds, first, second) {
            Ok(valid) => valid,
            Err(error) => {
                self.rollback(token);
                return Err(error);
            }
        };
        let (node, spare, load, is_second) = match (self, first, second) {
            (slots, None, None) => {
                slots.rollback(token);
                return Ok(Publish::Empty);
            }
            (slots @ (Self::Empty | Self::Second(_)), Some(first), second) => {
                return Self::reject(
                    slots,
                    error::Mismatch::First(limit::Mismatch::Empty {
                        actual: first.geometry().wire(),
                    }),
                    bounds,
                    Some(Value::Checked(first)),
                    second.map(Value::Checked),
                    token,
                );
            }
            (slots @ (Self::Empty | Self::First(_)), first, Some(second)) => {
                return Self::reject(
                    slots,
                    error::Mismatch::UnexpectedSecond {
                        wire: second.geometry().wire(),
                        resident: second.geometry().resident(),
                    },
                    bounds,
                    first.map(Value::Checked),
                    Some(Value::Checked(second)),
                    token,
                );
            }
            (Self::First(node), Some(load), None) => (node, None, load, false),
            (Self::Second(node), None, Some(load)) => (node, None, load, true),
            (Self::Pair(first, second), Some(load), None) => (first, Some(second), load, false),
            (Self::Pair(first, second), None, Some(load)) => (second, Some(first), load, true),
            (Self::Pair(left, right), Some(first), Some(second)) => {
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
                a.publish(token);
                b.publish(token);
                return Ok(Publish::Published);
            }
        };
        let limit = if is_second {
            bounds.second()
        } else {
            bounds.first()
        };
        let commit = match bind(node, load, limit) {
            Ok(commit) => commit,
            Err((node, load, cause)) => {
                let (slots, cause, first, second) = if is_second {
                    let slots = match spare {
                        Some(first) => Self::Pair(first, node),
                        None => Self::Second(node),
                    };
                    (
                        slots,
                        error::Mismatch::Second(cause),
                        None,
                        Some(Value::Checked(load)),
                    )
                } else {
                    let slots = match spare {
                        Some(second) => Self::Pair(node, second),
                        None => Self::First(node),
                    };
                    (
                        slots,
                        error::Mismatch::First(cause),
                        Some(Value::Checked(load)),
                        None,
                    )
                };
                return Self::reject(slots, cause, bounds, first, second, token);
            }
        };
        if let Some(node) = spare {
            node.rollback(token);
        }
        commit.publish(token);
        Ok(Publish::Published)
    }

    fn reject(
        self,
        cause: error::Mismatch,
        bounds: Bounds,
        first: Option<Value<B>>,
        second: Option<Value<B>>,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> Result<Publish, Error<B>> {
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
