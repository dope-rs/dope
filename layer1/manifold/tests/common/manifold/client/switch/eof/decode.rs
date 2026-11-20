use std::{cell::Cell, rc::Rc};

use dope_manifold::client::session::codec::{Codec, Stateful, staging::Buffered, storage};
use o3::buffer::{
    self,
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
}

impl Decode {
    pub(super) fn parsed(&self, state: &mut ParseState) -> Result<(), switch::Error> {
        state.parses += 1;
        let parses = self.parses.get();
        if state.parses != parses + 1 || parses != 0 {
            return Err(switch::Error::ParseCount {
                actual: state.parses,
                recorded: parses,
            });
        }
        self.parses.set(parses + 1);
        Ok(())
    }

    pub(super) fn finish(
        &self,
        state: &mut ParseState,
        bytes: &[u8],
    ) -> Result<Head, switch::Error> {
        if bytes != b"T" {
            return Err(switch::Error::TerminalBytes(bytes.to_vec()));
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
        Ok(Head)
    }
}
