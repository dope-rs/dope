pub trait Policy {
    /// Whether listener egress causally gates subsequent receive delivery.
    const EGRESS_COUPLED: bool = false;

    /// Whether the application owns a causal receive gate.
    const APPLICATION_GATED: bool = false;
}
