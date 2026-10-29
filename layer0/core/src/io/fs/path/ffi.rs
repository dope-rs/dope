pub(super) fn finish(bytes: Vec<u8>) -> Box<std::ffi::CStr> {
    // SAFETY: callers provide one terminal NUL after rejecting every interior NUL.
    unsafe { std::ffi::CString::from_vec_with_nul_unchecked(bytes) }.into_boxed_c_str()
}
