use crate::failure;

pub(crate) enum Fiber {
    Driver,
    Facade,
    Arrow,
    Body,
    Modifier,
    Move,
    AsyncFn,
    ArgumentCapture,
    MacroAwait,
    Expressions,
    Matches,
    Vec,
    AsyncScope,
    Pin,
    MacroControl,
    Attribute,
}

impl Fiber {
    pub(super) fn code(&self) -> &str {
        match self {
            Self::Driver => "DopeFiberDriver",
            Self::Facade => "DopeFiberFacade",
            Self::Arrow => "DopeFiberArrow",
            Self::Body => "DopeFiberBody",
            Self::Modifier => "DopeFiberModifier",
            Self::Move => "DopeFiberMove",
            Self::AsyncFn => "DopeFiberAsyncFn",
            Self::ArgumentCapture => "DopeFiberArgumentCapture",
            Self::MacroAwait => "DopeFiberMacroAwait",
            Self::Expressions => "DopeFiberExpressions",
            Self::Matches => "DopeFiberMatches",
            Self::Vec => "DopeFiberVec",
            Self::AsyncScope => "DopeFiberAsyncScope",
            Self::Pin => "DopeFiberPin",
            Self::MacroControl => "DopeFiberMacroControl",
            Self::Attribute => "DopeFiberAttribute",
        }
    }
}

impl From<Fiber> for failure::Kind {
    fn from(kind: Fiber) -> Self {
        Self::Fiber(kind)
    }
}
