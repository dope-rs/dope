use std::io;

pub enum Operation {}

pub enum Done {
    Complete,
    Failed(io::Error),
}
