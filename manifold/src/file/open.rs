use std::io;

use crate::file;

pub enum Operation {}

pub enum Done {
    Opened(file::Regular),
    Failed(io::Error),
}
