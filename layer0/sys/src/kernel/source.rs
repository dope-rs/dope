/// Native kernel outcomes retained without erasing their operation or payload.
pub trait Source {
    type Code: Copy + Eq;
    type Failure<S: Copy + Eq>: Copy + Eq + From<crate::Errno> + Into<super::Diagnostic<S>>;
    type Returned: Copy + Eq;
}
