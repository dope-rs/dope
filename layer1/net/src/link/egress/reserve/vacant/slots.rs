use o3::base::cell::region;

use crate::link::egress::{
    NonEmpty, data,
    load::{Value, credit},
    metadata::pool::{self, store::index::Index},
    reserve::{
        Geometry, Publish, demand,
        error::{self, Error},
        guard::Rollback,
        limit,
    },
};

#[must_use = "reserved slots must be committed or rolled back"]
pub(in crate::link::egress::reserve) enum Slots<
    'queue,
    'p,
    'd,
    B: data::raw::Payload<'d>,
    D: demand::Mode,
    G: Geometry,
> {
    Empty,
    Reserved(Reserved<'queue, 'p, 'd, B, D>, G::Indices<Index<'d>>),
}

type Reserved<'queue, 'p, 'd, B, D> = credit::Reserve<
    'queue,
    'p,
    'd,
    B,
    credit::Ready<'p, 'd>,
    D,
    <B as data::raw::Payload<'d>>::Position,
>;

fn validate<B, G: Geometry>(
    bounds: G,
    first: Option<Value<B>>,
    second: Option<Value<B>>,
) -> Result<(Option<NonEmpty<B>>, Option<NonEmpty<B>>), Error<B>> {
    let (first, second) = if first.is_none() && second.is_some() && !bounds.second().is_active() {
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
                bounds.bounds(),
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
                bounds.bounds(),
                first.map(Value::Checked),
                Some(Value::Unchecked(load)),
            ));
        }
    };
    Ok((first, second))
}

impl<'queue, 'p, 'd, B: data::raw::Payload<'d>, D: demand::Mode, G: Geometry>
    Slots<'queue, 'p, 'd, B, D, G>
{
    pub(super) fn try_publish(
        self,
        bounds: G,
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
        if first.is_none() && second.is_none() {
            self.rollback(token);
            return Ok(Publish::Empty);
        }
        let (credit, indices) = match self {
            Self::Empty => {
                let cause = match &first {
                    Some(first) => error::Mismatch::First(limit::Mismatch::Empty {
                        actual: first.geometry().wire(),
                    }),
                    None => {
                        let Some(second) = &second else {
                            unreachable!("loads");
                        };
                        error::Mismatch::UnexpectedSecond {
                            wire: second.geometry().wire(),
                            resident: second.geometry().resident(),
                        }
                    }
                };
                return Err(Error::changed(
                    cause,
                    bounds.bounds(),
                    first.map(Value::Checked),
                    second.map(Value::Checked),
                ));
            }
            Self::Reserved(credit, indices) => (credit, indices),
        };
        let (index, second_index, is_second) = G::parts(indices);
        if is_second {
            if let Some(first_load) = &first {
                let cause = error::Mismatch::First(limit::Mismatch::Empty {
                    actual: first_load.geometry().wire(),
                });
                return Self::reject(
                    Self::Reserved(credit, indices),
                    cause,
                    bounds,
                    first.map(Value::Checked),
                    second.map(Value::Checked),
                    token,
                );
            }
        }
        if !is_second && second_index.is_none() {
            if let Some(second_load) = &second {
                let cause = error::Mismatch::UnexpectedSecond {
                    wire: second_load.geometry().wire(),
                    resident: second_load.geometry().resident(),
                };
                return Self::reject(
                    Self::Reserved(credit, indices),
                    cause,
                    bounds,
                    first.map(Value::Checked),
                    second.map(Value::Checked),
                    token,
                );
            }
        }
        let (node, spare, load, is_second) = match second_index {
            Some(second_index) => {
                let mut credit = credit;
                let left = pool::Vacant::new(
                    credit::Claim::from(credit.split_pair(credit::Pair::new(
                        bounds.first().resident(),
                    ))),
                    index,
                    (),
                );
                let right = pool::Vacant::new(credit::Claim::from(credit), second_index, ());
                match (first, second) {
                    (Some(first), Some(second)) => {
                        let a = match bind::<_, _, G, false>(left, first, bounds) {
                            Ok(part) => part,
                            Err((left, first, cause)) => {
                                right.rollback(token);
                                left.rollback(token);
                                return Err(Error::changed(
                                    error::Mismatch::First(cause),
                                    bounds.bounds(),
                                    Some(Value::Checked(first)),
                                    Some(Value::Checked(second)),
                                ));
                            }
                        };
                        let b = match bind::<_, _, G, true>(right, second, bounds) {
                            Ok(part) => part,
                            Err((right, second, cause)) => {
                                let (left, first) = a.into_parts();
                                right.rollback(token);
                                left.rollback(token);
                                return Err(Error::changed(
                                    error::Mismatch::Second(cause),
                                    bounds.bounds(),
                                    Some(Value::Checked(first)),
                                    Some(Value::Checked(second)),
                                ));
                            }
                        };
                        a.publish(token);
                        b.publish(token);
                        return Ok(Publish::Published);
                    }
                    (Some(load), None) => (left, Some(right), load, false),
                    (None, Some(load)) => (right, Some(left), load, true),
                    (None, None) => unreachable!("loads"),
                }
            }
            None => {
                let load = match (first, second) {
                    (Some(load), None) | (None, Some(load)) => load,
                    _ => unreachable!("loads"),
                };
                (
                    pool::Vacant::new(credit::Claim::from(credit), index, ()),
                    None,
                    load,
                    is_second,
                )
            }
        };
        let bound = if is_second {
            bind::<_, _, G, true>(node, load, bounds)
        } else {
            bind::<_, _, G, false>(node, load, bounds)
        };
        let commit = match bound {
            Ok(commit) => commit,
            Err((node, load, cause)) => {
                let (cause, first, second) = if is_second {
                    node.rollback(token);
                    if let Some(first) = spare {
                        first.rollback(token);
                    }
                    (
                        error::Mismatch::Second(cause),
                        None,
                        Some(Value::Checked(load)),
                    )
                } else {
                    if let Some(second) = spare {
                        second.rollback(token);
                    }
                    node.rollback(token);
                    (
                        error::Mismatch::First(cause),
                        Some(Value::Checked(load)),
                        None,
                    )
                };
                return Err(Error::changed(cause, bounds.bounds(), first, second));
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
        bounds: G,
        first: Option<Value<B>>,
        second: Option<Value<B>>,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> Result<Publish, Error<B>> {
        let error = Error::changed(cause, bounds.bounds(), first, second);
        self.rollback(token);
        Err(error)
    }
}

fn bind<'queue, 'p, 'd, B: data::raw::Payload<'d>, D: demand::Mode, G: Geometry, const SECOND: bool>(
    node: pool::Vacant<'queue, 'p, 'd, B, (), credit::Ready<'p, 'd>, D, B::Position>,
    load: NonEmpty<B>,
    bounds: G,
) -> Result<pool::Bound<'queue, 'p, 'd, B, D, B::Position>, BindFailure<'queue, 'p, 'd, B, D>> {
    let limit = if SECOND { bounds.second() } else { bounds.first() };
    if let Err(cause) = limit.check(load.geometry().wire()) {
        return Err((node, load, cause));
    }
    node.try_bind(load)
        .map_err(|(node, load, cause)| (node, load, limit::Mismatch::Credit(cause)))
}

impl<'p, 'd, B: data::raw::Payload<'d>, D: demand::Mode, G: Geometry> Rollback<'d>
    for Slots<'_, 'p, 'd, B, D, G>
{
    fn rollback(self, token: &mut region::Token<region::Identity<'d>>) {
        let (credit, indices) = match self {
            Self::Empty => return,
            Self::Reserved(credit, indices) => (credit, indices),
        };
        let (first, second, _) = G::parts(indices);
        let queue = credit.queue();
        if let Some(second) = second {
            queue.pool.store.recycle(token, second);
        }
        queue.pool.store.recycle(token, first);
        credit.rollback(token);
    }
}

type BindFailure<'queue, 'p, 'd, B, D> = (
    pool::Vacant<
        'queue,
        'p,
        'd,
        B,
        (),
        credit::Ready<'p, 'd>,
        D,
        <B as data::raw::Payload<'d>>::Position,
    >,
    NonEmpty<B>,
    limit::Mismatch,
);
