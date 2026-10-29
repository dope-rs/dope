use crate::failure;

pub(crate) enum Work {
    Setting,
    DuplicateCore,
    DuplicateRuntime,
    Generics,
    Enum,
    Field,
    Count,
}

impl Work {
    pub(super) fn code(&self) -> &str {
        match self {
            Self::Setting => "DopeWorkSetting",
            Self::DuplicateCore => "DopeWorkDuplicateCore",
            Self::DuplicateRuntime => "DopeWorkDuplicateRuntime",
            Self::Generics => "DopeWorkGenerics",
            Self::Enum => "DopeWorkEnum",
            Self::Field => "DopeWorkField",
            Self::Count => "DopeWorkCount",
        }
    }
}

impl From<Work> for failure::Kind {
    fn from(kind: Work) -> Self {
        Self::Work(kind)
    }
}
