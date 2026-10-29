pub trait Marker {}

impl Marker for super::Borrowed {}
impl Marker for super::Coupled {}
impl Marker for super::Scoped {}
impl Marker for super::Retained {}
