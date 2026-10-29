/// Every failed admission retains its exact ring and provided-buffer owner.
#[derive(Debug)]
pub enum Error<R, P, O, G, A, E> {
    Open { ring: R, error: O },
    Registration { error: G, producer: P },
    Provided { error: E, ring: A, producer: P },
}
