use dope_core::driver::token::{Epoch, SlotIndex, Token};
use o3::marker::ThreadBound;

#[derive(Default)]
enum State {
    #[default]
    Idle,
    NeedsRearm,
    Multishoted,
    Retired,
}

#[derive(Default)]
pub struct Multishot {
    state: State,
    epoch: Epoch,
    _thread: ThreadBound,
}

impl Multishot {
    pub fn begin(&mut self, route: u8, slot: SlotIndex) -> Option<Token> {
        if matches!(self.state, State::Multishoted | State::Retired) {
            return None;
        }
        let Some(epoch) = self.epoch.next() else {
            self.state = State::Retired;
            return None;
        };
        self.epoch = epoch;
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
        token.slot() == slot && self.epoch == token.epoch()
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
        self.state = State::Idle;
    }
}
