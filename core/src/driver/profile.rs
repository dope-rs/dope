/// Static sizing and backend behavior used to construct a driver.
pub trait DriverProfile: 'static {
    const RING_ENTRIES: u32;
    const CQ_ENTRIES: u32 = 65536;
    const DEFER_TASKRUN: bool = true;

    const FIXED_FILE_SLOTS: u32 = 65535;
    const OUTBOUND_RESERVE: u32 = 256;
    const PROVIDED_BUF_ENTRIES: u16 = 4096;
    const PROVIDED_BUF_LEN: usize = 4096;

    const READY_SLOTS: usize = 1024;
}
