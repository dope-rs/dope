use o3::buffer::segments::scatter;

pub type Saved = (scatter::raw::Position, ());

pub trait State: sealed::Sealed + Copy + Default {
    type Join<R: State>: State;

    fn unit(&mut self) -> &mut ();
    fn saved(position: &mut Saved) -> &mut Self;
    fn left<R: State>(position: &mut Self::Join<R>) -> &mut Self;
    fn right<R: State>(position: &mut Self::Join<R>) -> &mut R;
}

impl State for () {
    type Join<R: State> = R;

    fn unit(&mut self) -> &mut () {
        self
    }

    fn saved(position: &mut Saved) -> &mut Self {
        &mut position.1
    }

    fn left<R: State>(position: &mut R) -> &mut Self {
        position.unit()
    }

    fn right<R: State>(position: &mut R) -> &mut R {
        position
    }
}

impl State for Saved {
    type Join<R: State> = Self;

    fn unit(&mut self) -> &mut () {
        &mut self.1
    }

    fn saved(position: &mut Saved) -> &mut Self {
        position
    }

    fn left<R: State>(position: &mut Self) -> &mut Self {
        position
    }

    fn right<R: State>(position: &mut Self) -> &mut R {
        R::saved(position)
    }
}

mod sealed {
    pub trait Sealed {}

    impl Sealed for () {}
    impl Sealed for super::Saved {}
}

const _: () = {
    assert!(size_of::<Saved>() == size_of::<scatter::raw::Position>());
    assert!(align_of::<Saved>() == align_of::<scatter::raw::Position>());
};
