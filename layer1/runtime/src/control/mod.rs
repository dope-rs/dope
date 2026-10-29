mod flow;
mod step;

pub use flow::Flow;
pub use step::Step;

/// The concrete coordination fields borrowed from one application step.
pub trait Fields<'step, 'd: 'step> {
    type View;
}
