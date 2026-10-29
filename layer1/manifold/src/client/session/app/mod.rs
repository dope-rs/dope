//! Canonical protocol-session application adapter.

pub mod admit;
pub mod drain;
mod ingress;
mod parser;
pub mod request;
mod retire;
mod state;

pub use state::State;
