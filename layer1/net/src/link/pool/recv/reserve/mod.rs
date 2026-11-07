mod choice;
mod plan;

pub use choice::Choice;

use crate::link::{
    event,
    pool::recv::{delivery, input},
};

pub type Reservation<'a, 'w, 'd, ID, W, M> = event::Reservation<
    'd,
    ID,
    delivery::Data<'a, 'w, 'd, ID, W, <M as input::Mode<W>>::Kind, input::Receipt<W, M>>,
>;
