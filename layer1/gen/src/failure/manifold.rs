use crate::failure;

pub(crate) enum Manifold {
    Crate,
    DuplicateCrate,
    Packed,
    Lifetime,
    Mark,
    Driver,
    Struct,
    Named,
    FieldCount,
    NamedField,
    FieldMark,
    FieldPin,
    ForwardArgs,
}

impl Manifold {
    pub(super) fn code(&self) -> &str {
        match self {
            Self::Crate => "DopeManifoldCrate",
            Self::DuplicateCrate => "DopeManifoldDuplicateCrate",
            Self::Packed => "DopeManifoldPacked",
            Self::Lifetime => "DopeManifoldLifetime",
            Self::Mark => "DopeManifoldMark",
            Self::Driver => "DopeManifoldDriver",
            Self::Struct => "DopeManifoldStruct",
            Self::Named => "DopeManifoldNamed",
            Self::FieldCount => "DopeManifoldFieldCount",
            Self::NamedField => "DopeManifoldNamedField",
            Self::FieldMark => "DopeManifoldFieldMark",
            Self::FieldPin => "DopeManifoldFieldPin",
            Self::ForwardArgs => "DopeManifoldForwardArgs",
        }
    }
}

impl From<Manifold> for failure::Kind {
    fn from(kind: Manifold) -> Self {
        Self::Manifold(kind)
    }
}
