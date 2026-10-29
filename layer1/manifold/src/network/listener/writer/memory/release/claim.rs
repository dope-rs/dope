pub(in crate::network::listener) enum Claim<T> {
    Acquired(T),
    Waiting,
}
