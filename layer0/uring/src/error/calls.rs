#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Setup {}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Map {}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Unmap {}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Enter {}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Register<const OPCODE: u32> {}
