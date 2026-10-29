pub mod codec;
mod cookies;
mod extent;
pub use extent::Extent;
pub mod fixture;
pub mod lookup;
pub mod refresh;
pub mod stream;

type Owner<'q, 'h, 'p, 'life, 's, 'd, R> = dope_dns::Resolver<
    'q,
    'h,
    'p,
    'life,
    's,
    'd,
    KeyTag<{ refresh::DATAGRAM_ROUTE }>,
    KeyTag<{ refresh::STREAM_ROUTE }>,
    { refresh::LANES },
    { refresh::ADDRESSES },
    R,
>;
use dope::core::driver::route::KeyTag;
