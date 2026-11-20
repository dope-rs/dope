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

use crate::common::{contract::Report, manifold::client::error};

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
        staging::Plan::new(2, 2)
    }
}

impl Stateful for Decode {
    type State<'d> = State;

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
}

impl Decode {
    pub(super) fn decode<'d, S, T>(
        &self,
        storage: &mut Storage<S>,
        state: &mut State,
        budget: &resident::Budget<'d>,
        limit: Limit,
        deliver: impl FnOnce(Result<Head, Error>) -> T,
    ) -> codec::Step<T>
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
        T: response::Outcome,
    {
        codec::Input::commit(&mut storage.source, budget, limit, |input| {
            if input.as_slice() == b"P" || input.as_slice() == b"Q" {
                if storage.partials != 0 {
                    return Err(deliver(Err(Error::Prefix(storage.partials))));
                }
                storage.partials += 1;
                let used = if input.as_slice() == b"P" {
                    match input.try_consumed(NonZero::<usize>::MIN) {
                        Ok(used) => Some(used),
                        Err(cause) => return Err(deliver(Err(Error::Consume(cause)))),
                    }
                } else {
                    None
                };
                self.prefix.hit();
                return Ok(match input.need_more(used) {
                    Some(need) => {
                        self.growth.hit();
                        Parse::More(need)
                    }
                    None => Parse::CapacityExhausted,
                });
            }
            let prefixes = self.prefix.hits();
            if storage.partials != prefixes as usize {
                return Err(deliver(Err(Error::Rebound {
                    partials: storage.partials,
                    prefixes,
                })));
            }
            state.parses += 1;
            if state.parses > self.attempts.get() {
                return Err(deliver(Err(Error::Parses {
                    actual: state.parses,
                    allowed: self.attempts,
                })));
            }
            let Some(&byte) = input.first() else {
                return Ok(match input.need_more(None) {
                    Some(need) => Parse::More(need),
                    None => Parse::CapacityExhausted,
                });
            };
            let count = match byte {
                b'H' => NonZero::<usize>::MIN,
                b'Q' if input.as_slice() == b"QQ" => NonZero::<usize>::MIN.saturating_add(1),
                _ => return Err(deliver(Err(Error::Head(byte)))),
            };
            let consumed = match input.try_consumed(count) {
                Ok(consumed) => consumed,
                Err(cause) => return Err(deliver(Err(Error::Consume(cause)))),
            };
            let head = Head {
                suffix: input.as_ptr().wrapping_add(count.get()),
            };
            Ok(Parse::Item {
                head: deliver(Ok(head)),
                consumed,
            })
        })
    }

    pub(super) fn finish<'slot, 'id, S, T: response::Outcome>(
        &self,
        mut input: storage::Owner<'slot, 'id, Storage<S>, State>,
        audit: &Report<error::Error, 16>,
    ) -> codec::Finished<storage::Owner<'slot, 'id, Storage<S>, State>, T> {
        let (_, state) = input.parts();
        let result = if state.parses > self.attempts.get() {
            audit.faults.record(error::Error::Decode(Error::Parses {
                actual: state.parses,
                allowed: self.attempts,
            }));
            Err(T::committed())
        } else {
            Ok(codec::Finish::Empty)
        };
        (input, result)
    }
}
