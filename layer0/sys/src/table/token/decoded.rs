/// Classified kernel identity before binding it to its owning domain.
pub enum Decoded<K, R, F, T> {
    Close(R),
    Framework(F),
    Flight(K),
    CancelFlight(K),
    CancelTuning(T),
    Invalid,
}
