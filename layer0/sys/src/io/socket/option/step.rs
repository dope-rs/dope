use std::ops;

use crate::io::socket::option::{Kind, Value};

/// One scalar and its position in a strongly ordered option chain.
pub trait Step {
    fn kind(&self) -> ops::ControlFlow<Kind, Kind>;
    fn value(&self) -> &Value;
}
