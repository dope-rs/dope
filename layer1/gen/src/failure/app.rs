use crate::failure;

pub(crate) enum App {
    OptionalType,
    StorageForm,
    ControlOwner,
    DuplicateControl,
    UnknownOwner,
    Named,
    CoordinateField,
    CoordinateStep,
    CoordinateHeader,
    PinRequired,
    DuplicatePin,
    PinShape,
    DuplicateDrain,
    DrainShape,
    OwnerRequired,
    Lifecycle,
    EmptyOwner,
    OwnerShape,
    DrainOwned,
    Struct,
    DriverLifetime,
    Drain,
    DuplicateRouteName,
    ControlCoordinate,
    Dispatcher,
    DispatcherOptions,
    UnknownDispatcher,
    FacadeMix,
    Core,
    Manifold,
    Runtime,
    Coordinate,
    DuplicateCoordinate,
    Duplicate(Setting),
    DuplicateState,
    StateShape,
    StateRole,
    StateName,
    StateConflict,
}

#[derive(Clone, Copy)]
pub(crate) enum Setting {
    Crate,
    Core,
    Manifold,
    Runtime,
    Driver,
}

impl App {
    pub(super) fn code(&self) -> &str {
        match self {
            Self::OptionalType => "DopeAppOptionalType",
            Self::StorageForm => "DopeAppStorageForm",
            Self::ControlOwner => "DopeAppControlOwner",
            Self::DuplicateControl => "DopeAppDuplicateControl",
            Self::UnknownOwner => "DopeAppUnknownOwner",
            Self::Named => "DopeAppNamed",
            Self::CoordinateField => "DopeAppCoordinateField",
            Self::CoordinateStep => "DopeAppCoordinateStep",
            Self::CoordinateHeader => "DopeAppCoordinateHeader",
            Self::PinRequired => "DopeAppPinRequired",
            Self::DuplicatePin => "DopeAppDuplicatePin",
            Self::PinShape => "DopeAppPinShape",
            Self::DuplicateDrain => "DopeAppDuplicateDrain",
            Self::DrainShape => "DopeAppDrainShape",
            Self::OwnerRequired => "DopeAppOwnerRequired",
            Self::Lifecycle => "DopeAppLifecycle",
            Self::EmptyOwner => "DopeAppEmptyOwner",
            Self::OwnerShape => "DopeAppOwnerShape",
            Self::DrainOwned => "DopeAppDrainOwned",
            Self::Struct => "DopeAppStruct",
            Self::DriverLifetime => "DopeAppDriverLifetime",
            Self::Drain => "DopeAppDrain",
            Self::DuplicateRouteName => "DopeAppDuplicateRouteName",
            Self::ControlCoordinate => "DopeAppControlCoordinate",
            Self::Dispatcher => "DopeAppDispatcher",
            Self::DispatcherOptions => "DopeAppDispatcherOptions",
            Self::UnknownDispatcher => "DopeAppUnknownDispatcher",
            Self::FacadeMix => "DopeAppFacadeMix",
            Self::Core => "DopeAppCore",
            Self::Manifold => "DopeAppManifold",
            Self::Runtime => "DopeAppRuntime",
            Self::Coordinate => "DopeAppCoordinate",
            Self::DuplicateCoordinate => "DopeAppDuplicateCoordinate",
            Self::Duplicate(Setting::Crate) => "DopeAppDuplicateCrate",
            Self::Duplicate(Setting::Core) => "DopeAppDuplicateCore",
            Self::Duplicate(Setting::Manifold) => "DopeAppDuplicateManifold",
            Self::Duplicate(Setting::Runtime) => "DopeAppDuplicateRuntime",
            Self::Duplicate(Setting::Driver) => "DopeAppDuplicateDriver",
            Self::DuplicateState => "DopeAppDuplicateState",
            Self::StateShape => "DopeAppStateShape",
            Self::StateRole => "DopeAppStateRole",
            Self::StateName => "DopeAppStateName",
            Self::StateConflict => "DopeAppStateConflict",
        }
    }
}

impl From<App> for failure::Kind {
    fn from(kind: App) -> Self {
        Self::App(kind)
    }
}
