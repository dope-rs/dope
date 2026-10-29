use crate::app::dispatch;

/// # Safety
/// After stopped quiescence, native retirement leaves no state awaiting dispatch before finish.
/// Later dependent destruction must leave cleanup valid when the same driver resumes.
pub unsafe trait Resume<'d>: dispatch::raw::Manifold<'d> {}
