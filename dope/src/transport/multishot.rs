use crate::backend;

#[derive(Default)]
enum State {
    #[default]
    Idle,
    NeedsRearm,
    Armed,
}

#[derive(Default)]
pub struct Arm {
    state: State,
    epoch: backend::token::Epoch,
}

impl Arm {
    pub fn begin(
        &mut self,
        route: u8,
        slot_id: backend::token::LocalIdx,
    ) -> Option<backend::token::Token> {
        if matches!(self.state, State::Armed) {
            return None;
        }
        self.epoch = self.epoch.bump();
        Some(backend::token::Token::new(route, slot_id, self.epoch))
    }

    pub fn settle(&mut self, pushed: bool) {
        self.state = if pushed {
            State::Armed
        } else {
            State::NeedsRearm
        };
    }

    pub fn on_completion(&mut self, more: bool) {
        if !more && matches!(self.state, State::Armed) {
            self.state = State::NeedsRearm;
        }
    }

    pub fn epoch_match(
        &self,
        ud: backend::token::Token,
        expected_slot_id: backend::token::LocalIdx,
    ) -> bool {
        ud.slot() == expected_slot_id && self.epoch == ud.epoch()
    }

    pub fn request_rearm(&mut self) {
        self.state = State::NeedsRearm;
    }

    pub fn needs_rearm(&self) -> bool {
        matches!(self.state, State::NeedsRearm)
    }

    pub fn is_armed(&self) -> bool {
        matches!(self.state, State::Armed)
    }

    pub fn current_epoch(&self) -> backend::token::Epoch {
        self.epoch
    }

    pub fn quiesce(&mut self) {
        self.state = State::Idle;
    }
}
