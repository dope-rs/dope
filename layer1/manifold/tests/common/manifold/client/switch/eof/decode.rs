use std::{cell::Cell, rc::Rc};

use dope_manifold::client::session::{
    codec::{
        self, Codec, Parse, Stateful,
        staging::{Buffered, Limit},
        storage,
    },
    response,
};
use o3::buffer::{
    self, hold,
    resident::{self, staging},
};

use crate::common::manifold::client::switch;

pub struct Decode {
    pub(crate) parses: Rc<Cell<usize>>,
    pub(crate) finishes: Rc<Cell<usize>>,
}

pub struct Head;

pub struct ParseState {
    pub(crate) parses: usize,
    pub(crate) finishes: usize,
}

impl Codec for Decode {
    type Head<'input, 'd, R: buffer::Cursor>
        = Head
    where
        R: 'input,
        'd: 'input;
    type Error = switch::Error;
    type Staging = Buffered;

    fn staging(&self) -> Result<staging::Plan, staging::Error> {
        staging::Plan::new(1, 2)
    }
}

impl Stateful for Decode {
    type State<'d> = ParseState;

    fn state<'d>(&self, _: resident::Budget<'d>) -> ParseState {
        ParseState {
            parses: 0,
            finishes: 0,
        }
    }

    type Storage<S> = S;
    type Preview<'a> = &'a Head;
    type Outcome<'d> = Head;

    fn storage<'d, S>(&self, source: S, _: &ParseState) -> S {
        source
    }

    fn source<S: buffer::raw::Source>(storage: &S) -> &S {
        storage
    }

    fn edit<S>(storage: &mut S) -> storage::Edit<'_, S> {
        storage::Edit::new(storage)
    }

    fn restart<'a, 'd, S>(&self, storage: &'a mut S, _: &ParseState) -> storage::Edit<'a, S> {
        Self::edit(storage)
    }

    fn decode<'d, S, T, F>(
        &self,
        storage: &mut S,
        state: &mut ParseState,
        budget: &resident::Budget<'d>,
        limit: Limit,
        admit: impl FnOnce(Result<Self::Preview<'_>, &Self::Error>) -> codec::Admission<F>,
    ) -> codec::Step<T>
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
        T: response::Outcome,
        F: FnOnce(Result<Self::Outcome<'d>, Self::Error>, &mut ParseState) -> T::Accepted,
    {
        codec::Input::commit(storage, budget, limit, |input| {
            switch::parsed(self.parse_once(state, input), state, admit)
        })
    }

    fn finish<'slot, 'id, 'd, S, T, F>(
        &self,
        mut input: storage::Owner<'slot, 'id, S, ParseState>,
        budget: &resident::Budget<'d>,
        limit: Limit,
        admit: impl FnOnce(Result<Self::Preview<'_>, &Self::Error>) -> codec::Admission<F>,
    ) -> (
        storage::Owner<'slot, 'id, S, ParseState>,
        Result<codec::Finish<T>, T>,
    )
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
        T: response::Outcome,
        F: FnOnce(Result<Self::Outcome<'d>, Self::Error>, &mut ParseState) -> T::Accepted,
    {
        let (storage, state) = input.parts();
        let result = codec::Input::with(storage, budget, limit, |input| {
            switch::finished(self.finish_once(state, input), state, admit)
        });
        (input, result)
    }
}

impl Decode {
    fn parse_once<'id, 'input, 'd, R: buffer::Cursor>(
        &self,
        state: &mut ParseState,
        input: codec::Input<'id, 'input, 'd, R>,
    ) -> Result<Parse<'id, Head>, switch::Error>
    where
        'd: 'input,
    {
        state.parses += 1;
        let parses = self.parses.get();
        if state.parses != parses + 1 || parses != 0 {
            return Err(switch::Error::ParseCount {
                actual: state.parses,
                recorded: parses,
            });
        }
        self.parses.set(parses + 1);
        Ok(match input.need_more(None) {
            Some(need) => Parse::More(need),
            None => Parse::CapacityExhausted,
        })
    }

    fn finish_once<'id, 'input, 'd, R: buffer::Cursor>(
        &self,
        state: &mut ParseState,
        input: codec::Input<'id, 'input, 'd, R>,
    ) -> Result<codec::Finish<Head>, switch::Error>
    where
        'd: 'input,
    {
        if input.as_slice() != b"T" {
            return Err(switch::Error::TerminalBytes(input.as_slice().to_vec()));
        }
        state.finishes += 1;
        let finishes = self.finishes.get();
        if state.finishes != finishes + 1 || finishes >= 2 {
            return Err(switch::Error::FinishCount {
                actual: state.finishes,
                recorded: finishes,
            });
        }
        self.finishes.set(finishes + 1);
        Ok(codec::Finish::Item(Head))
    }
}
