pub enum Closed<E, C> {
    Ready(E),
    Missing,
    Class { echo: E, expected: C, observed: C },
}
