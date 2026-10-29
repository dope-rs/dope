use std::num::NonZero;

use dope_manifold::client::session::{
    codec::{
        self, Codec, Parse, Stateful,
        staging::{Buffered, Limit},
        storage,
    },
    response,
};
use dope_test::harness::signals::{Gate, Progress as _};
use o3::{
    buffer,
    buffer::{
        hold,
        resident::{self, staging},
        write::consume::scope,
    },
};

pub struct Decode {
    pub(crate) attempts: NonZero<usize>,
    pub(crate) prefix: Gate,
    pub(crate) growth: Gate,
}

pub struct Storage<S> {
    source: S,
    partials: usize,
}

pub struct Head {
    pub(crate) suffix: *const u8,
}

pub struct State {
    pub(crate) parses: usize,
}

#[derive(Debug)]
pub enum Error {
    Head(u8),
    Prefix(usize),
    Rebound {
        partials: usize,
        prefixes: u32,
    },
    Parses {
        actual: usize,
        allowed: NonZero<usize>,
    },
    Consume(scope::Error),
}

impl Codec for Decode {
    type Head<'input, 'd, R: buffer::Cursor>
        = Head
    where
        R: 'input,
        'd: 'input;
    type Error = Error;
    type Staging = Buffered;

    fn staging(&self) -> Result<staging::Plan, staging::Error> {
        staging::Plan::new(1, 1)
    }
}

impl Stateful for Decode {
    type State<'d> = State;
    type Preview<'a> = &'a Head;
    type Outcome<'d> = Head;

    fn state<'d>(&self, _: resident::Budget<'d>) -> State {
        State { parses: 0 }
    }

    type Storage<S> = Storage<S>;

    fn storage<'d, S>(&self, source: S, _: &State) -> Storage<S> {
        Storage {
            source,
            partials: 0,
        }
    }

    fn source<S: buffer::raw::Source>(storage: &Storage<S>) -> &S {
        &storage.source
    }
    fn edit<S>(storage: &mut Storage<S>) -> storage::Edit<'_, S> {
        storage::Edit::new(&mut storage.source)
    }

    fn restart<'a, 'd, S>(&self, storage: &'a mut Storage<S>, _: &State) -> storage::Edit<'a, S> {
        storage.partials = 0;
        Self::edit(storage)
    }

    fn decode<'d, S, T, F>(
        &self,
        storage: &mut Storage<S>,
        state: &mut State,
        budget: &resident::Budget<'d>,
        limit: Limit,
        admit: impl FnOnce(Result<Self::Preview<'_>, &Self::Error>) -> codec::Admission<F>,
    ) -> codec::Step<T>
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
        T: response::Outcome,
        F: FnOnce(Result<Self::Outcome<'d>, Self::Error>, &mut State) -> T::Accepted,
    {
        let emit = |result, state| super::accepted(result, state, admit);
        codec::Input::commit(&mut storage.source, budget, limit, |input| {
            if input.as_slice() == b"P" {
                if storage.partials != 0 {
                    return Err(emit(Err(Error::Prefix(storage.partials)), state));
                }
                storage.partials += 1;
                let used = match input.try_consumed(NonZero::<usize>::MIN) {
                    Ok(used) => used,
                    Err(cause) => return Err(emit(Err(Error::Consume(cause)), state)),
                };
                self.prefix.hit();
                return Ok(match input.need_more(Some(used)) {
                    Some(need) => {
                        self.growth.hit();
                        Parse::More(need)
                    }
                    None => Parse::CapacityExhausted,
                });
            }
            let prefixes = self.prefix.hits();
            if storage.partials != prefixes as usize {
                return Err(emit(
                    Err(Error::Rebound {
                        partials: storage.partials,
                        prefixes,
                    }),
                    state,
                ));
            }
            state.parses += 1;
            if state.parses > self.attempts.get() {
                return Err(emit(
                    Err(Error::Parses {
                        actual: state.parses,
                        allowed: self.attempts,
                    }),
                    state,
                ));
            }
            let Some(&byte) = input.first() else {
                return Ok(match input.need_more(None) {
                    Some(need) => Parse::More(need),
                    None => Parse::CapacityExhausted,
                });
            };
            if byte != b'H' {
                return Err(emit(Err(Error::Head(byte)), state));
            }
            let consumed = match input.try_consumed(NonZero::<usize>::MIN) {
                Ok(consumed) => consumed,
                Err(cause) => return Err(emit(Err(Error::Consume(cause)), state)),
            };
            let head = Head {
                suffix: input.as_ptr().wrapping_add(1),
            };
            Ok(Parse::Item {
                head: emit(Ok(head), state),
                consumed,
            })
        })
    }

    fn finish<'slot, 'id, 'd, S, T, F>(
        &self,
        mut input: storage::Owner<'slot, 'id, Storage<S>, State>,
        _: &resident::Budget<'d>,
        _: Limit,
        admit: impl FnOnce(Result<Self::Preview<'_>, &Self::Error>) -> codec::Admission<F>,
    ) -> (
        storage::Owner<'slot, 'id, Storage<S>, State>,
        Result<codec::Finish<T>, T>,
    )
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
        T: response::Outcome,
        F: FnOnce(Result<Self::Outcome<'d>, Self::Error>, &mut State) -> T::Accepted,
    {
        let (_, state) = input.parts();
        let emit = |result, state| super::accepted(result, state, admit);
        let result = if state.parses > self.attempts.get() {
            Err(emit(
                Err(Error::Parses {
                    actual: state.parses,
                    allowed: self.attempts,
                }),
                state,
            ))
        } else {
            Ok(codec::Finish::Empty)
        };
        (input, result)
    }
}
