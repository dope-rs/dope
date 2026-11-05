/// Thread-local CPU-affinity capabilities.
pub mod affinity;

/// Process-signal acquisition and scoped waiting.
pub mod signal;

/// Borrowed descriptor readiness with owned poll storage.
pub mod poll;

/// Scoped ownership of worker inputs, native threads, and joined outputs.
pub mod thread;
