use dope::runtime::civil;
use o3::{
    alloc,
    depot::{capacity, index::bitmap},
};

use crate::config;

#[derive(Debug)]
pub enum Error {
    Config(config::Error),
    Lanes(capacity::Error),
    Branches(capacity::Error),
    Transactions(capacity::Error),
    Allocation(alloc::Error),
    Entries(bitmap::Error),
    Clock(civil::Error),
}
