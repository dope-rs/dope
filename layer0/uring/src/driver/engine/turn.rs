pub struct Turn<'a, H, Q> {
    pub(in crate::driver) host: &'a mut H,
    pub(in crate::driver) quota: Q,
}
