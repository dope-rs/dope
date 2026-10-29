pub struct Root;

#[repr(transparent)]
pub struct External<F>(pub(super) F);

pub trait Source<'life, P: ?Sized> {
    type Sink: ?Sized;

    fn get<'a>(&'a self, root: &'life P) -> &'a Self::Sink
    where
        'life: 'a;
}

impl<'life, P: ?Sized> Source<'life, P> for Root {
    type Sink = P;

    fn get<'a>(&'a self, root: &'life P) -> &'a P
    where
        'life: 'a,
    {
        root
    }
}

impl<'life, P: ?Sized, F> Source<'life, P> for External<F> {
    type Sink = F;

    fn get<'a>(&'a self, _: &'life P) -> &'a F
    where
        'life: 'a,
    {
        &self.0
    }
}
