#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Empty,
    Parent {
        offset: usize,
    },
    NoComponent {
        length: usize,
    },
    Nested {
        offset: usize,
    },
    Component {
        offset: usize,
        length: usize,
        limit: usize,
    },
    Nul {
        position: usize,
    },
}
