use dope_core::driver::token::{Epoch, SlotIndex, Token};
use o3::marker::ThreadBound;

enum State {
    Fresh,
    Idle,
    NeedsRearm,
    Multishoted,
    Retired,
}

pub struct Multishot {
    state: State,
    epoch: Epoch,
    _thread: ThreadBound,
}

impl Default for Multishot {
    fn default() -> Self {
        Self {
            state: State::Fresh,
            epoch: Epoch::INITIAL,
            _thread: ThreadBound::NEW,
        }
    }
}

impl Multishot {
    pub fn begin(&mut self, route: u8, slot: SlotIndex) -> Option<Token> {
        if matches!(self.state, State::Multishoted | State::Retired) {
            return None;
        }
        if !matches!(self.state, State::Fresh) {
            let Some(epoch) = self.epoch.next() else {
                self.state = State::Retired;
                return None;
            };
            self.epoch = epoch;
        }
        Some(Token::new(route, slot, self.epoch))
    }

    pub fn settle(&mut self, pushed: bool) {
        self.state = if pushed {
            State::Multishoted
        } else {
            State::NeedsRearm
        };
    }

    pub fn complete(&mut self, more: bool) {
        if !more && matches!(self.state, State::Multishoted) {
            self.state = State::NeedsRearm;
        }
    }

    pub fn epoch_match(&self, token: Token, slot: SlotIndex) -> bool {
        token.slot() == slot && token.epoch() == Some(self.epoch)
    }

    pub fn request_rearm(&mut self) {
        self.state = State::NeedsRearm;
    }

    pub fn needs_rearm(&self) -> bool {
        matches!(self.state, State::NeedsRearm)
    }

    pub fn is_armed(&self) -> bool {
        matches!(self.state, State::Multishoted)
    }

    pub fn current_epoch(&self) -> Epoch {
        self.epoch
    }

    pub fn quiesce(&mut self) {
        if !matches!(self.state, State::Fresh) {
            self.state = State::Idle;
        }
    }
}
