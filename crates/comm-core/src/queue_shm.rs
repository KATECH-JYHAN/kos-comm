// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Jun-young Han <jyhan@katech.re.kr>
// KATECH SDV Platform Research Center

use crate::error::{Error, Result};

use core::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::ffi::CString;
use std::ptr;

#[cfg(feature = "cache-line-32")]
pub const CACHE_LINE: usize = 32;
#[cfg(feature = "cache-line-128")]
pub const CACHE_LINE: usize = 128;

#[cfg(not(any(feature = "cache-line-32", feature = "cache-line-128")))]
pub const CACHE_LINE: usize = {
    #[cfg(target_arch = "arm")]
    { 32 }
    #[cfg(all(target_arch = "aarch64", target_vendor = "apple"))]
    { 128 }
    #[cfg(not(any(
        target_arch = "arm",
        all(target_arch = "aarch64", target_vendor = "apple"),
    )))]
    { 64 }
};

const CL_MASK: u32 = (CACHE_LINE as u32) - 1;

macro_rules! cache_aligned {
    ($(#[$meta:meta])* $vis:vis struct $name:ident { $($body:tt)* }) => {
        $(#[$meta])*
        #[cfg_attr(feature = "cache-line-32", repr(C, align(32)))]
        #[cfg_attr(feature = "cache-line-128", repr(C, align(128)))]
        #[cfg_attr(all(
            not(any(feature = "cache-line-32", feature = "cache-line-128")),
            target_arch = "arm",
        ), repr(C, align(32)))]
        #[cfg_attr(all(
            not(any(feature = "cache-line-32", feature = "cache-line-128")),
            target_arch = "aarch64",
            target_vendor = "apple",
        ), repr(C, align(128)))]
        #[cfg_attr(all(
            not(any(feature = "cache-line-32", feature = "cache-line-128")),
            not(target_arch = "arm"),
            not(all(target_arch = "aarch64", target_vendor = "apple")),
        ), repr(C, align(64)))]
        $vis struct $name { $($body)* }
    };
}

pub const QUEUE_SHM_MAGIC: u32 = 0x5155_3730;

pub const QUEUE_SHM_VERSION: u32 = 2;

pub const MAX_DATA_SHMS: usize = 64;

pub const MAX_READERS: usize = 16;

const SLOTS_OFFSET: usize = core::mem::size_of::<QueueShmHeader>();

#[inline]
pub fn monotonic_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe {
        libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts);
    }
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

fn fnv1a_hash(topic: &str) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for &b in topic.as_bytes() {
        h ^= b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

pub fn queue_shm_name(topic: &str) -> String {
    if cfg!(target_os = "android") {
        format!("kos_q_{:08x}", fnv1a_hash(topic))
    } else {
        format!("/kos_q_{:08x}", fnv1a_hash(topic))
    }
}

#[inline]
pub fn futex_notify(header: &QueueShmHeader) {
    header.notify_efd.fetch_add(1, Ordering::Release);
    platform_notify(header);
}

#[inline]
pub fn futex_wait_for_publish(header: &QueueShmHeader, cached: &mut u32, timeout_ns: u64) -> bool {
    for _ in 0..100 {
        let cur = header.notify_efd.load(Ordering::Acquire);
        if cur != *cached {
            *cached = cur;
            return true;
        }
        core::hint::spin_loop();
    }

    platform_wait(header, *cached, timeout_ns);

    let cur = header.notify_efd.load(Ordering::Acquire);
    if cur != *cached {
        *cached = cur;
        return true;
    }
    false
}

#[cfg(target_os = "linux")]
#[inline]
fn platform_notify(_header: &QueueShmHeader) {
    unsafe {
        libc::syscall(
            libc::SYS_futex,
            &_header.notify_efd as *const AtomicU32,
            libc::FUTEX_WAKE,
            i32::MAX,
            core::ptr::null::<libc::timespec>(),
        );
    }
}

#[cfg(target_os = "linux")]
#[inline]
fn platform_wait(_header: &QueueShmHeader, expected: u32, timeout_ns: u64) {
    let ts = libc::timespec {
        tv_sec: (timeout_ns / 1_000_000_000) as libc::time_t,
        tv_nsec: (timeout_ns % 1_000_000_000) as libc::c_long,
    };
    unsafe {
        libc::syscall(
            libc::SYS_futex,
            &_header.notify_efd as *const AtomicU32,
            libc::FUTEX_WAIT,
            expected as i32,
            &ts as *const libc::timespec,
        );
    }
}

#[cfg(target_os = "macos")]
extern "C" {
    fn os_sync_wait_on_address(
        addr: *const core::ffi::c_void,
        value: u64,
        size: usize,
        flags: u32,
    ) -> i32;
    fn os_sync_wake_by_address_all(
        addr: *const core::ffi::c_void,
        size: usize,
        flags: u32,
    ) -> i32;
}

#[cfg(target_os = "macos")]
const OS_SYNC_WAIT_ON_ADDRESS_NONE: u32 = 0;

#[cfg(target_os = "macos")]
#[inline]
fn platform_notify(_header: &QueueShmHeader) {
    unsafe {
        os_sync_wake_by_address_all(
            &_header.notify_efd as *const AtomicU32 as *const core::ffi::c_void,
            core::mem::size_of::<u32>(),
            OS_SYNC_WAIT_ON_ADDRESS_NONE,
        );
    }
}

#[cfg(target_os = "macos")]
#[inline]
fn platform_wait(_header: &QueueShmHeader, expected: u32, _timeout_ns: u64) {
    unsafe {
        os_sync_wait_on_address(
            &_header.notify_efd as *const AtomicU32 as *const core::ffi::c_void,
            expected as u64,
            core::mem::size_of::<u32>(),
            OS_SYNC_WAIT_ON_ADDRESS_NONE,
        );
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
#[inline]
fn platform_notify(_header: &QueueShmHeader) {
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
#[inline]
fn platform_wait(_header: &QueueShmHeader, expected: u32, timeout_ns: u64) {
    let deadline = {
        let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
        unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts); }
        (ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64) + timeout_ns
    };
    loop {
        let cur = _header.notify_efd.load(Ordering::Acquire);
        if cur != expected { return; }
        let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
        unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts); }
        let now = ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64;
        if now >= deadline { return; }
        std::thread::yield_now();
    }
}

pub fn queue_hdr_shm_name(topic: &str) -> String {
    queue_shm_name(topic)
}

cache_aligned! {
    pub struct ReaderBitmask {
        pub slots: AtomicU64,
        _pad: [u8; CACHE_LINE - 8],
    }
}

impl core::fmt::Debug for ReaderBitmask {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "ReaderBitmask({:#x})", self.slots.load(Ordering::Relaxed))
    }
}

const _: () = assert!(core::mem::size_of::<ReaderBitmask>() == CACHE_LINE);

const CL0_DATA: usize = 8 + 4 + 4 + 4 + 4 + 8 + 4 + 4 + 4;
const CL0_SIZE: usize = CL0_DATA.div_ceil(CACHE_LINE) * CACHE_LINE;
const CL0_PAD: usize = CL0_SIZE - CL0_DATA;

const CL1_DATA: usize = 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4;
const CL1_SIZE: usize = CL1_DATA.div_ceil(CACHE_LINE) * CACHE_LINE;
const CL1_PAD: usize = CL1_SIZE - CL1_DATA;

const CL_STATS_DATA: usize = 8 + 8 + 8 + 8;
const CL_STATS_SIZE: usize = CL_STATS_DATA.div_ceil(CACHE_LINE) * CACHE_LINE;
const CL_STATS_PAD: usize = CL_STATS_SIZE - CL_STATS_DATA;

const CL2_TAIL_DATA: usize = MAX_DATA_SHMS * 4 + MAX_DATA_SHMS + 4;
const CL2_TAIL_SIZE: usize = CL2_TAIL_DATA.div_ceil(CACHE_LINE) * CACHE_LINE;
const CL2_TAIL_PAD: usize = CL2_TAIL_SIZE - CL2_TAIL_DATA;

const EXPECTED_HEADER_SIZE: usize =
    CL0_SIZE + CL1_SIZE + CL_STATS_SIZE + CL2_TAIL_SIZE + MAX_READERS * CACHE_LINE;

cache_aligned! {
    pub struct QueueShmHeader {
        pub publish_count: AtomicU64,
        pub latest_data_idx: AtomicU32,
        pub queue_len: AtomicU32,
        pub pos_write: AtomicU32,
        pub queue_head: AtomicU32,
        pub heartbeat_ns: AtomicU64,
        pub publisher_pid: AtomicU32,
        pub notify_efd: AtomicU32,
        pub notify_sub_count: AtomicU32,
        _pad_cl0: [u8; CL0_PAD],

        pub magic: u32,
        pub version: u32,
        pub history: u32,
        pub margin: u32,
        pub num: AtomicU32,
        pub data_size: u32,
        pub threshold: u32,
        pub deadline_us: u32,
        _pad_cl1: [u8; CL1_PAD],

        pub pub_overflow_count: AtomicU64,
        pub pub_starvation_count: AtomicU64,
        pub sub_total_miss: AtomicU64,
        pub stats_start_ns: AtomicU64,
        _pad_stats: [u8; CL_STATS_PAD],

        pub queue: [AtomicU32; MAX_DATA_SHMS],
        pub pending_flags: [AtomicU8; MAX_DATA_SHMS],
        pub next_reader_id: AtomicU32,
        _pad_rid: [u8; CL2_TAIL_PAD],
        pub reader_bitmasks: [ReaderBitmask; MAX_READERS],
    }
}

const _: () = assert!(core::mem::size_of::<QueueShmHeader>() == EXPECTED_HEADER_SIZE);

impl QueueShmHeader {
    #[inline]
    pub fn any_reader_active(&self, slot_idx: u32) -> bool {
        let bit = 1u64 << slot_idx;
        for i in 0..MAX_READERS {
            if self.reader_bitmasks[i].slots.load(Ordering::Acquire) & bit != 0 {
                return true;
            }
        }
        false
    }

    #[inline]
    pub fn clear_all_readers(&self, slot_idx: u32) {
        let mask = !(1u64 << slot_idx);
        for i in 0..MAX_READERS {
            self.reader_bitmasks[i].slots.fetch_and(mask, Ordering::Release);
        }
    }

    pub fn alloc_reader_id(&self) -> Option<u32> {
        let id = self.next_reader_id.fetch_add(1, Ordering::AcqRel);
        if id < MAX_READERS as u32 {
            Some(id)
        } else {
            None
        }
    }
}

const DATA_HDR_DATA: usize = 4 + 4 + 8 + 4 + 4 + 8 + 8;
const DATA_HDR_SIZE: usize = DATA_HDR_DATA.div_ceil(CACHE_LINE) * CACHE_LINE;
const DATA_HDR_PAD: usize = DATA_HDR_SIZE - DATA_HDR_DATA;

cache_aligned! {
    #[derive(Debug)]
    pub struct ShmDataHeader {
        pub rx_count: AtomicU32,
        pub cnt_skip: AtomicU32,
        pub cnt_read: AtomicU64,
        pub size: AtomicU32,
        pub generation: AtomicU32,
        pub timestamp: AtomicU64,
        pub sequence: AtomicU64,
        _pad0: [u8; DATA_HDR_PAD],
    }
}

const _: () = assert!(core::mem::size_of::<ShmDataHeader>() == DATA_HDR_SIZE);

fn slot_stride(data_size: u32) -> Result<u32> {
    let hdr = core::mem::size_of::<ShmDataHeader>() as u32;
    let raw = hdr.checked_add(data_size).ok_or(Error::InvalidConfig)?;
    let aligned = raw.checked_add(CL_MASK).ok_or(Error::InvalidConfig)?;
    Ok(aligned & !CL_MASK)
}

fn total_shm_size(num_slots: u32, data_size: u32) -> Result<usize> {
    Ok(SLOTS_OFFSET + num_slots as usize * slot_stride(data_size)? as usize)
}

pub struct QueueShm {
    fd: libc::c_int,
    addr: *mut u8,
    total_size: usize,
    #[allow(dead_code)]
    name: CString,
    is_owner: bool,
    pub(crate) readonly: bool,
    #[allow(dead_code)]
    topic: String,
    stride: u32,
    num_slots: u32,
    #[cfg(feature = "ivshmem")]
    is_ivshmem: bool,
}

impl core::fmt::Debug for QueueShm {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("QueueShm")
            .field("total_size", &self.total_size)
            .field("is_owner", &self.is_owner)
            .field("stride", &self.stride)
            .finish()
    }
}

unsafe impl Send for QueueShm {}
unsafe impl Sync for QueueShm {}

impl QueueShm {
    pub fn header(&self) -> &QueueShmHeader {
        unsafe { &*(self.addr as *const QueueShmHeader) }
    }

    pub fn data_size(&self) -> u32 {
        self.header().data_size
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn deadline_us(&self) -> u32 {
        self.header().deadline_us
    }

    #[inline]
    pub fn slot_header(&self, index: u32) -> Result<&ShmDataHeader> {
        if index >= self.num_slots {
            return Err(Error::DataIndexOutOfRange);
        }
        Ok(unsafe { self.slot_header_unchecked(index) })
    }

    #[inline(always)]
    pub unsafe fn slot_header_unchecked(&self, index: u32) -> &ShmDataHeader {
        let offset = SLOTS_OFFSET + index as usize * self.stride as usize;
        &*(self.addr.add(offset) as *const ShmDataHeader)
    }

    #[inline]
    pub fn slot_data_ptr(&self, index: u32) -> Result<*mut u8> {
        if index >= self.num_slots {
            return Err(Error::DataIndexOutOfRange);
        }
        Ok(unsafe { self.slot_data_ptr_unchecked(index) })
    }

    #[inline(always)]
    pub unsafe fn slot_data_ptr_unchecked(&self, index: u32) -> *mut u8 {
        let offset = SLOTS_OFFSET
            + index as usize * self.stride as usize
            + core::mem::size_of::<ShmDataHeader>();
        self.addr.add(offset)
    }

    #[inline(always)]
    pub fn data_count(&self) -> u32 {
        self.num_slots
    }

    #[cfg(not(target_os = "android"))]
    fn is_existing_publisher_alive(c_name: &CString) -> bool {
        unsafe {
            let fd = libc::shm_open(c_name.as_ptr(), libc::O_RDONLY, 0);
            if fd < 0 {
                return false;
            }
            let hdr_size = core::mem::size_of::<QueueShmHeader>();
            let mut stat: libc::stat = core::mem::zeroed();
            if libc::fstat(fd, &mut stat) != 0 || (stat.st_size as usize) < hdr_size {
                libc::close(fd);
                return false;
            }
            let addr = libc::mmap(
                ptr::null_mut(),
                hdr_size,
                libc::PROT_READ,
                libc::MAP_SHARED,
                fd,
                0,
            );
            libc::close(fd);
            if addr == libc::MAP_FAILED {
                return false;
            }
            let hdr = &*(addr as *const QueueShmHeader);
            let alive = if hdr.magic == QUEUE_SHM_MAGIC {
                let pid = hdr.publisher_pid.load(Ordering::Acquire);
                pid != 0 && (libc::kill(pid as i32, 0) == 0
                    || *libc::__errno_location() == libc::EPERM)
            } else {
                false
            };
            libc::munmap(addr, hdr_size);
            alive
        }
    }

    #[cfg(all(not(target_os = "android"), feature = "ivshmem"))]
    fn create_ivshmem(
        topic: &str,
        history: u32,
        margin: u32,
        data_size: u32,
        threshold: u32,
        deadline_us: u32,
    ) -> Result<Self> {
        if data_size == 0 || history == 0 {
            return Err(Error::InvalidConfig);
        }
        let num_slots = history.checked_add(margin).ok_or(Error::InvalidConfig)?;
        if num_slots as usize > MAX_DATA_SHMS {
            return Err(Error::InvalidConfig);
        }

        let stride = slot_stride(data_size)?;
        let total_size = total_shm_size(num_slots, data_size)?;

        let mapped = crate::backend::ivshmem::create_mapping(topic, total_size, 0)?;

        if mapped.size < total_size {
            return Err(Error::ShmCreateFailed);
        }

        unsafe { ptr::write_bytes(mapped.ptr, 0, total_size) };

        let hdr_ptr = mapped.ptr as *mut QueueShmHeader;
        unsafe {
            ptr::addr_of_mut!((*hdr_ptr).magic).write(QUEUE_SHM_MAGIC);
            ptr::addr_of_mut!((*hdr_ptr).version).write(QUEUE_SHM_VERSION);
            ptr::addr_of_mut!((*hdr_ptr).history).write(history);
            ptr::addr_of_mut!((*hdr_ptr).margin).write(margin);
            ptr::addr_of_mut!((*hdr_ptr).data_size).write(data_size);
            ptr::addr_of_mut!((*hdr_ptr).threshold).write(threshold);
            ptr::addr_of_mut!((*hdr_ptr).deadline_us).write(deadline_us);
        }

        let hdr = unsafe { &*hdr_ptr };
        hdr.num.store(num_slots, Ordering::Release);
        hdr.publisher_pid.store(std::process::id(), Ordering::Release);
        hdr.heartbeat_ns.store(0, Ordering::Release);
        hdr.publish_count.store(0, Ordering::Release);
        hdr.latest_data_idx.store(u32::MAX, Ordering::Release);
        hdr.queue_len.store(0, Ordering::Release);
        hdr.pos_write.store(0, Ordering::Release);
        hdr.queue_head.store(0, Ordering::Release);

        for i in 0..MAX_DATA_SHMS {
            hdr.queue[i].store(u32::MAX, Ordering::Release);
            hdr.pending_flags[i].store(0, Ordering::Release);
        }

        Ok(QueueShm {
            fd: mapped.fd,
            addr: mapped.ptr,
            total_size: mapped.size,
            name: CString::new(topic).map_err(|_| Error::ShmCreateFailed)?,
            is_owner: true,
            readonly: false,
            topic: topic.to_string(),
            stride,
            num_slots,
            is_ivshmem: true,
        })
    }

    #[cfg(not(target_os = "android"))]
    pub fn create(
        topic: &str,
        history: u32,
        margin: u32,
        data_size: u32,
        threshold: u32,
        deadline_us: u32,
    ) -> Result<Self> {
        #[cfg(feature = "ivshmem")]
        if crate::backend::ivshmem::is_initialized() {
            return Self::create_ivshmem(topic, history, margin, data_size, threshold, deadline_us);
        }
        Self::create_posix(topic, history, margin, data_size, threshold, deadline_us)
    }

    #[cfg(not(target_os = "android"))]
    fn create_posix(
        topic: &str,
        history: u32,
        margin: u32,
        data_size: u32,
        threshold: u32,
        deadline_us: u32,
    ) -> Result<Self> {
        if data_size == 0 || history == 0 {
            return Err(Error::InvalidConfig);
        }
        let num_slots = history.checked_add(margin).ok_or(Error::InvalidConfig)?;
        if num_slots as usize > MAX_DATA_SHMS {
            return Err(Error::InvalidConfig);
        }

        let stride = slot_stride(data_size)?;
        let total_size = total_shm_size(num_slots, data_size)?;

        let shm_name = queue_shm_name(topic);
        let c_name = CString::new(shm_name).map_err(|_| Error::ShmCreateFailed)?;

        let fd = unsafe {
            let fd = libc::shm_open(
                c_name.as_ptr(),
                libc::O_CREAT | libc::O_EXCL | libc::O_RDWR,
                0o600,
            );
            if fd < 0 {
                let errno = *libc::__errno_location();
                if errno == libc::EEXIST {
                    let existing_alive = Self::is_existing_publisher_alive(&c_name);
                    if existing_alive {
                        return Err(Error::ShmCreateFailed);
                    }
                    libc::shm_unlink(c_name.as_ptr());
                    let fd2 = libc::shm_open(
                        c_name.as_ptr(),
                        libc::O_CREAT | libc::O_EXCL | libc::O_RDWR,
                        0o600,
                    );
                    if fd2 < 0 {
                        return Err(Error::ShmCreateFailed);
                    }
                    fd2
                } else {
                    return Err(Error::ShmCreateFailed);
                }
            } else {
                fd
            }
        };

        if unsafe { libc::ftruncate(fd, total_size as libc::off_t) } != 0 {
            unsafe {
                libc::close(fd);
                libc::shm_unlink(c_name.as_ptr());
            }
            return Err(Error::ShmCreateFailed);
        }

        let addr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                total_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };

        if addr == libc::MAP_FAILED {
            unsafe {
                libc::close(fd);
                libc::shm_unlink(c_name.as_ptr());
            }
            return Err(Error::ShmCreateFailed);
        }

        unsafe { ptr::write_bytes(addr as *mut u8, 0, total_size) };

        let hdr_ptr = addr as *mut QueueShmHeader;
        unsafe {
            ptr::addr_of_mut!((*hdr_ptr).magic).write(QUEUE_SHM_MAGIC);
            ptr::addr_of_mut!((*hdr_ptr).version).write(QUEUE_SHM_VERSION);
            ptr::addr_of_mut!((*hdr_ptr).history).write(history);
            ptr::addr_of_mut!((*hdr_ptr).margin).write(margin);
            ptr::addr_of_mut!((*hdr_ptr).data_size).write(data_size);
            ptr::addr_of_mut!((*hdr_ptr).threshold).write(threshold);
            ptr::addr_of_mut!((*hdr_ptr).deadline_us).write(deadline_us);
        }

        let hdr = unsafe { &*hdr_ptr };
        hdr.num.store(num_slots, Ordering::Release);
        hdr.publisher_pid.store(std::process::id(), Ordering::Release);
        hdr.heartbeat_ns.store(0, Ordering::Release);
        hdr.publish_count.store(0, Ordering::Release);
        hdr.latest_data_idx.store(u32::MAX, Ordering::Release);
        hdr.queue_len.store(0, Ordering::Release);
        hdr.pos_write.store(0, Ordering::Release);
        hdr.queue_head.store(0, Ordering::Release);

        for i in 0..MAX_DATA_SHMS {
            hdr.queue[i].store(u32::MAX, Ordering::Release);
            hdr.pending_flags[i].store(0, Ordering::Release);
        }

        Ok(QueueShm {
            fd,
            addr: addr as *mut u8,
            total_size,
            name: c_name,
            is_owner: true,
            readonly: false,
            topic: topic.to_string(),
            stride,
            num_slots,
            #[cfg(feature = "ivshmem")]
            is_ivshmem: false,
        })
    }

    #[cfg(all(not(target_os = "android"), feature = "ivshmem"))]
    fn attach_inner_ivshmem(topic: &str, readonly: bool) -> Result<Self> {
        let mapped = crate::backend::ivshmem::attach_mapping(topic, 0)?;

        if mapped.size < core::mem::size_of::<QueueShmHeader>() {
            return Err(Error::QueueValidationFailed);
        }

        let hdr = unsafe { &*(mapped.ptr as *const QueueShmHeader) };
        if hdr.magic != QUEUE_SHM_MAGIC || hdr.version != QUEUE_SHM_VERSION {
            return Err(Error::QueueValidationFailed);
        }

        let stride = slot_stride(hdr.data_size)?;
        let num_slots = hdr.num.load(Ordering::Acquire);
        if num_slots as usize > MAX_DATA_SHMS {
            return Err(Error::QueueValidationFailed);
        }

        let expected_size = total_shm_size(num_slots, hdr.data_size)?;
        if mapped.size < expected_size {
            return Err(Error::QueueValidationFailed);
        }

        Ok(QueueShm {
            fd: mapped.fd,
            addr: mapped.ptr,
            total_size: mapped.size,
            name: CString::new(topic).map_err(|_| Error::ShmCreateFailed)?,
            is_owner: false,
            readonly,
            topic: topic.to_string(),
            stride,
            num_slots,
            is_ivshmem: true,
        })
    }

    #[cfg(not(target_os = "android"))]
    pub fn attach(topic: &str) -> Result<Self> {
        Self::attach_dispatch(topic, false)
    }

    #[cfg(not(target_os = "android"))]
    pub fn attach_readonly(topic: &str) -> Result<Self> {
        Self::attach_dispatch(topic, true)
    }

    #[cfg(not(target_os = "android"))]
    fn attach_dispatch(topic: &str, readonly: bool) -> Result<Self> {
        #[cfg(feature = "ivshmem")]
        if crate::backend::ivshmem::is_initialized() {
            return Self::attach_inner_ivshmem(topic, readonly);
        }
        Self::attach_inner(topic, readonly)
    }

    #[cfg(not(target_os = "android"))]
    fn attach_inner(topic: &str, readonly: bool) -> Result<Self> {
        let shm_name = queue_shm_name(topic);
        let c_name = CString::new(shm_name).map_err(|_| Error::ShmAttachFailed)?;

        let oflags = if readonly {
            libc::O_RDONLY
        } else {
            libc::O_RDWR
        };
        let fd = unsafe { libc::shm_open(c_name.as_ptr(), oflags, 0) };
        if fd < 0 {
            return Err(Error::ShmAttachFailed);
        }

        let total_size = unsafe {
            let mut stat: libc::stat = core::mem::zeroed();
            if libc::fstat(fd, &mut stat) != 0 {
                libc::close(fd);
                return Err(Error::ShmAttachFailed);
            }
            stat.st_size as usize
        };

        if total_size < core::mem::size_of::<QueueShmHeader>() {
            unsafe { libc::close(fd) };
            return Err(Error::QueueValidationFailed);
        }

        let prot = if readonly {
            libc::PROT_READ
        } else {
            libc::PROT_READ | libc::PROT_WRITE
        };

        let addr =
            unsafe { libc::mmap(ptr::null_mut(), total_size, prot, libc::MAP_SHARED, fd, 0) };

        if addr == libc::MAP_FAILED {
            unsafe { libc::close(fd) };
            return Err(Error::ShmAttachFailed);
        }

        let hdr = unsafe { &*(addr as *const QueueShmHeader) };
        if hdr.magic != QUEUE_SHM_MAGIC || hdr.version != QUEUE_SHM_VERSION {
            unsafe {
                libc::munmap(addr, total_size);
                libc::close(fd);
            }
            return Err(Error::QueueValidationFailed);
        }

        let stride = match slot_stride(hdr.data_size) {
            Ok(s) => s,
            Err(e) => {
                unsafe {
                    libc::munmap(addr, total_size);
                    libc::close(fd);
                }
                return Err(e);
            }
        };
        let num_slots = hdr.num.load(Ordering::Acquire);

        if num_slots as usize > MAX_DATA_SHMS {
            unsafe {
                libc::munmap(addr, total_size);
                libc::close(fd);
            }
            return Err(Error::QueueValidationFailed);
        }

        let expected_size = match total_shm_size(num_slots, hdr.data_size) {
            Ok(s) => s,
            Err(e) => {
                unsafe {
                    libc::munmap(addr, total_size);
                    libc::close(fd);
                }
                return Err(e);
            }
        };
        if total_size < expected_size {
            unsafe {
                libc::munmap(addr, total_size);
                libc::close(fd);
            }
            return Err(Error::QueueValidationFailed);
        }

        Ok(QueueShm {
            fd,
            addr: addr as *mut u8,
            total_size,
            name: c_name,
            is_owner: false,
            readonly,
            topic: topic.to_string(),
            stride,
            num_slots,
            #[cfg(feature = "ivshmem")]
            is_ivshmem: false,
        })
    }
}

#[cfg(not(target_os = "android"))]
pub fn cleanup_stale_queue(topic: &str) -> Result<bool> {
    let shm_name = queue_shm_name(topic);
    let c_name = CString::new(shm_name).map_err(|_| Error::ShmAttachFailed)?;

    let fd = unsafe { libc::shm_open(c_name.as_ptr(), libc::O_RDONLY, 0) };
    if fd < 0 {
        return Ok(false);
    }

    let file_size = unsafe {
        let mut stat: libc::stat = core::mem::zeroed();
        if libc::fstat(fd, &mut stat) != 0 {
            libc::close(fd);
            return Ok(false);
        }
        stat.st_size as usize
    };

    if file_size < core::mem::size_of::<QueueShmHeader>() {
        unsafe {
            libc::close(fd);
            libc::shm_unlink(c_name.as_ptr());
        }
        return Ok(true);
    }

    let hdr_size = core::mem::size_of::<QueueShmHeader>();
    let addr = unsafe {
        libc::mmap(
            ptr::null_mut(),
            hdr_size,
            libc::PROT_READ,
            libc::MAP_SHARED,
            fd,
            0,
        )
    };
    unsafe { libc::close(fd) };

    if addr == libc::MAP_FAILED {
        return Ok(false);
    }

    let hdr = unsafe { &*(addr as *const QueueShmHeader) };

    if hdr.magic != QUEUE_SHM_MAGIC {
        unsafe {
            libc::munmap(addr, hdr_size);
            libc::shm_unlink(c_name.as_ptr());
        }
        return Ok(true);
    }

    let pid = hdr.publisher_pid.load(Ordering::Acquire);

    unsafe { libc::munmap(addr, hdr_size) };

    if pid == 0 {
        unsafe { libc::shm_unlink(c_name.as_ptr()) };
        return Ok(true);
    }

    let alive = unsafe {
        libc::kill(pid as i32, 0) == 0
            || *libc::__errno_location() == libc::EPERM
    };

    if !alive {
        unsafe { libc::shm_unlink(c_name.as_ptr()) };
        Ok(true)
    } else {
        Ok(false)
    }
}

impl Drop for QueueShm {
    fn drop(&mut self) {
        #[cfg(feature = "ivshmem")]
        if self.is_ivshmem {
            if self.is_owner {
                crate::backend::ivshmem::deactivate_topic(&self.topic);
            }
            return;
        }

        unsafe {
            libc::munmap(self.addr as *mut libc::c_void, self.total_size);
            libc::close(self.fd);
            #[cfg(not(target_os = "android"))]
            if self.is_owner {
                libc::shm_unlink(self.name.as_ptr());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn naming_convention() {
        let name = queue_shm_name("test/topic");
        assert!(name.starts_with("/kos_q_"));
    }

    #[test]
    fn naming_deterministic() {
        let a = queue_shm_name("vehicle/lidar");
        let b = queue_shm_name("vehicle/lidar");
        assert_eq!(a, b);
    }

    #[test]
    fn naming_different_topics() {
        let a = queue_shm_name("topic_a");
        let b = queue_shm_name("topic_b");
        assert_ne!(a, b);
    }

    #[test]
    fn header_size_check() {
        assert_eq!(core::mem::size_of::<QueueShmHeader>(), EXPECTED_HEADER_SIZE);
        assert_eq!(core::mem::size_of::<ShmDataHeader>(), DATA_HDR_SIZE);
        assert_eq!(core::mem::size_of::<ReaderBitmask>(), CACHE_LINE);
    }

    #[test]
    fn stride_alignment() {
        let hdr_size = core::mem::size_of::<ShmDataHeader>() as u32;
        let cl = CACHE_LINE as u32;

        let align_up = |data_size: u32| -> u32 {
            let raw = hdr_size + data_size;
            (raw + cl - 1) / cl * cl
        };

        assert_eq!(slot_stride(8).unwrap(), align_up(8));
        assert_eq!(slot_stride(48).unwrap(), align_up(48));
        assert_eq!(slot_stride(64).unwrap(), align_up(64));
        assert_eq!(slot_stride(256).unwrap(), align_up(256));

        for ds in [0, 1, 8, 48, 64, 128, 256, 1024] {
            let s = slot_stride(ds).unwrap();
            assert_eq!(s % cl, 0, "stride {s} not aligned to {cl}B for data_size={ds}");
        }
    }

    #[test]
    fn create_and_attach() {
        let topic = "test/queue_shm_create";
        let shm = QueueShm::create(topic, 4, 2, 256, 5, 0).unwrap();
        assert_eq!(shm.header().magic, QUEUE_SHM_MAGIC);
        assert_eq!(shm.header().version, QUEUE_SHM_VERSION);
        assert_eq!(shm.header().history, 4);
        assert_eq!(shm.header().margin, 2);
        assert_eq!(shm.header().data_size, 256);
        assert_eq!(shm.data_count(), 6);

        let expected = core::mem::size_of::<QueueShmHeader>() + 6 * slot_stride(256).unwrap() as usize;
        assert_eq!(shm.total_size, expected);

        let sub_shm = QueueShm::attach(topic).unwrap();
        assert_eq!(sub_shm.header().magic, QUEUE_SHM_MAGIC);
        assert_eq!(sub_shm.data_count(), 6);

        drop(sub_shm);
        drop(shm);
    }

    #[test]
    fn attach_readonly() {
        let topic = "test/queue_shm_ro";
        let shm = QueueShm::create(topic, 4, 2, 128, 5, 0).unwrap();

        let ro_shm = QueueShm::attach_readonly(topic).unwrap();
        assert_eq!(ro_shm.header().magic, QUEUE_SHM_MAGIC);
        assert!(ro_shm.readonly);

        drop(ro_shm);
        drop(shm);
    }

    #[test]
    fn slot_access() {
        let topic = "test/queue_shm_slot";
        let shm = QueueShm::create(topic, 2, 1, 64, 5, 0).unwrap();
        assert_eq!(shm.data_count(), 3);

        for i in 0..3u32 {
            let sh = shm.slot_header(i).unwrap();
            assert_eq!(sh.rx_count.load(Ordering::Relaxed), 0);
        }

        let p0 = shm.slot_data_ptr(0).unwrap();
        let p1 = shm.slot_data_ptr(1).unwrap();
        assert_ne!(p0, p1);
        assert_eq!(
            (p1 as usize) - (p0 as usize),
            shm.stride as usize
        );

        drop(shm);
    }

    #[test]
    fn slot_index_out_of_range() {
        let topic = "test/queue_shm_range";
        let shm = QueueShm::create(topic, 2, 0, 64, 5, 0).unwrap();

        assert!(shm.slot_header(0).is_ok());
        assert!(shm.slot_header(1).is_ok());
        assert_eq!(
            shm.slot_header(2).unwrap_err(),
            Error::DataIndexOutOfRange
        );
        assert_eq!(
            shm.slot_data_ptr(100).unwrap_err(),
            Error::DataIndexOutOfRange
        );

        drop(shm);
    }

    #[test]
    fn drop_cleans_up() {
        let topic = "test/queue_shm_drop";
        {
            let _shm = QueueShm::create(topic, 2, 1, 64, 5, 0).unwrap();
        }
        let result = QueueShm::attach(topic);
        assert!(result.is_err());
    }

    #[test]
    fn contiguous_memory() {
        let topic = "test/queue_shm_contig";
        let shm = QueueShm::create(topic, 4, 0, 64, 5, 0).unwrap();

        let base = shm.addr as usize;
        let end = base + shm.total_size;

        for i in 0..4u32 {
            let sh_ptr = shm.slot_header(i).unwrap() as *const ShmDataHeader as usize;
            let dp = shm.slot_data_ptr(i).unwrap() as usize;
            assert!(sh_ptr >= base && sh_ptr < end);
            assert!(dp >= base && dp < end);
        }

        drop(shm);
    }

    #[test]
    fn pending_flags_initialized() {
        let topic = "test/queue_shm_pending";
        let shm = QueueShm::create(topic, 4, 2, 128, 5, 0).unwrap();

        for i in 0..MAX_DATA_SHMS {
            assert_eq!(
                shm.header().pending_flags[i].load(Ordering::Relaxed),
                0,
                "pending flag {i} should be 0"
            );
        }

        drop(shm);
    }

    #[test]
    fn max_slots_limit() {
        let topic = "test/queue_shm_max";
        let shm = QueueShm::create(topic, 32, 32, 8, 5, 0).unwrap();
        assert_eq!(shm.data_count(), 64);
        drop(shm);

        let result = QueueShm::create("test/queue_shm_over", 33, 32, 8, 5, 0);
        assert_eq!(result.unwrap_err(), Error::InvalidConfig);
    }

    #[test]
    fn create_history_zero_rejected() {
        let result = QueueShm::create("test/queue_shm_h0", 0, 1, 64, 5, 0);
        assert_eq!(result.unwrap_err(), Error::InvalidConfig);
    }

    #[test]
    fn create_history_one() {
        let topic = "test/queue_shm_h1";
        let shm = QueueShm::create(topic, 1, 0, 64, 5, 0).unwrap();
        assert_eq!(shm.data_count(), 1);
        assert_eq!(shm.header().history, 1);
        drop(shm);
    }

    #[test]
    fn create_history_100_slots() {
        let topic = "test/queue_shm_h100";
        let shm = QueueShm::create(topic, 50, 14, 8, 5, 0).unwrap();
        assert_eq!(shm.data_count(), 64);
        drop(shm);
    }

    #[test]
    fn slot_write_and_read_back() {
        let topic = "test/queue_shm_wr";
        let shm = QueueShm::create(topic, 2, 1, 64, 5, 0).unwrap();

        let ptr = shm.slot_data_ptr(0).unwrap();
        unsafe {
            core::ptr::write_bytes(ptr, 0xAB, 64);
        }

        let read_ptr = shm.slot_data_ptr(0).unwrap();
        let first_byte = unsafe { *read_ptr };
        assert_eq!(first_byte, 0xAB);

        drop(shm);
    }

    #[test]
    fn lock_mode_reading_increment_decrement() {
        let topic = "test/queue_shm_lock";
        let shm = QueueShm::create(topic, 4, 2, 64, 5, 0).unwrap();

        let sh = shm.slot_header(0).unwrap();
        assert_eq!(sh.rx_count.load(Ordering::Relaxed), 0);

        sh.rx_count.fetch_add(1, Ordering::Acquire);
        assert_eq!(sh.rx_count.load(Ordering::Relaxed), 1);

        sh.rx_count.fetch_add(1, Ordering::Acquire);
        assert_eq!(sh.rx_count.load(Ordering::Relaxed), 2);

        sh.rx_count.fetch_sub(1, Ordering::Release);
        assert_eq!(sh.rx_count.load(Ordering::Relaxed), 1);

        sh.rx_count.fetch_sub(1, Ordering::Release);
        assert_eq!(sh.rx_count.load(Ordering::Relaxed), 0);

        drop(shm);
    }

    #[test]
    fn ring_mode_reading_untouched() {
        let topic = "test/queue_shm_ring";
        let shm = QueueShm::create(topic, 4, 2, 64, 5, 0).unwrap();

        let sh = shm.slot_header(0).unwrap();
        assert_eq!(sh.rx_count.load(Ordering::Relaxed), 0);

        let _ptr = shm.slot_data_ptr(0).unwrap();

        assert_eq!(sh.rx_count.load(Ordering::Relaxed), 0);

        drop(shm);
    }

    #[test]
    fn cnt_skip_counting() {
        let topic = "test/queue_shm_skip";
        let shm = QueueShm::create(topic, 4, 2, 64, 5, 0).unwrap();

        let sh = shm.slot_header(0).unwrap();
        assert_eq!(sh.cnt_skip.load(Ordering::Relaxed), 0);

        sh.cnt_skip.fetch_add(1, Ordering::Relaxed);
        sh.cnt_skip.fetch_add(1, Ordering::Relaxed);
        assert_eq!(sh.cnt_skip.load(Ordering::Relaxed), 2);

        let threshold = shm.header().threshold;
        assert_eq!(threshold, 5);
        assert!(sh.cnt_skip.load(Ordering::Relaxed) < threshold);

        sh.cnt_skip.fetch_add(3, Ordering::Relaxed);
        assert_eq!(sh.cnt_skip.load(Ordering::Relaxed), 5);
        assert!(sh.cnt_skip.load(Ordering::Relaxed) >= threshold);

        drop(shm);
    }

    #[test]
    fn generation_seqlock_pattern() {
        let topic = "test/queue_shm_gen";
        let shm = QueueShm::create(topic, 2, 1, 64, 5, 0).unwrap();

        let sh = shm.slot_header(0).unwrap();
        assert_eq!(sh.generation.load(Ordering::Relaxed), 0);

        sh.generation.fetch_add(1, Ordering::Release);
        assert_eq!(sh.generation.load(Ordering::Relaxed) & 1, 1);

        sh.generation.fetch_add(1, Ordering::Release);
        assert_eq!(sh.generation.load(Ordering::Relaxed) & 1, 0);
        assert_eq!(sh.generation.load(Ordering::Relaxed), 2);

        drop(shm);
    }

    #[test]
    fn queue_pos_write_advance() {
        let topic = "test/queue_shm_pos";
        let shm = QueueShm::create(topic, 4, 2, 64, 5, 0).unwrap();

        let hdr = shm.header();
        assert_eq!(hdr.pos_write.load(Ordering::Relaxed), 0);
        assert_eq!(hdr.queue_len.load(Ordering::Relaxed), 0);

        let num = shm.data_count();
        for i in 0..num {
            let pos = hdr.pos_write.load(Ordering::Relaxed);
            hdr.queue[pos as usize].store(i, Ordering::Release);
            hdr.pos_write.store((pos + 1) % num, Ordering::Release);
            hdr.queue_len.fetch_add(1, Ordering::Release);
        }

        assert_eq!(hdr.queue_len.load(Ordering::Relaxed), num);
        assert_eq!(hdr.pos_write.load(Ordering::Relaxed), 0);

        drop(shm);
    }

    #[test]
    fn publisher_pid_recorded() {
        let topic = "test/queue_shm_pid";
        let pid = unsafe { libc::getpid() } as u32;
        let shm = QueueShm::create(topic, 2, 1, 64, 5, pid).unwrap();
        assert_eq!(shm.header().publisher_pid.load(Ordering::Relaxed), pid);
        drop(shm);
    }

    #[test]
    fn publish_count_increments() {
        let topic = "test/queue_shm_pubcnt";
        let shm = QueueShm::create(topic, 2, 1, 64, 5, 0).unwrap();

        let hdr = shm.header();
        assert_eq!(hdr.publish_count.load(Ordering::Relaxed), 0);

        hdr.publish_count.fetch_add(1, Ordering::Release);
        hdr.publish_count.fetch_add(1, Ordering::Release);
        hdr.publish_count.fetch_add(1, Ordering::Release);
        assert_eq!(hdr.publish_count.load(Ordering::Relaxed), 3);

        drop(shm);
    }

    #[test]
    fn multi_thread_slot_access() {
        let topic = "test/queue_shm_mt";
        let shm = QueueShm::create(topic, 4, 2, 128, 5, 0).unwrap();

        let shm = std::sync::Arc::new(shm);
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(4));

        let mut handles = Vec::new();
        for tid in 0..4u32 {
            let s = shm.clone();
            let b = barrier.clone();
            handles.push(std::thread::spawn(move || {
                b.wait();
                let sh = s.slot_header(tid).unwrap();
                sh.rx_count.fetch_add(1, Ordering::Acquire);

                let ptr = s.slot_data_ptr(tid).unwrap();
                unsafe {
                    core::ptr::write_bytes(ptr, tid as u8, 128);
                }

                sh.rx_count.fetch_sub(1, Ordering::Release);

                let byte = unsafe { *s.slot_data_ptr(tid).unwrap() };
                assert_eq!(byte, tid as u8);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        for i in 0..4u32 {
            assert_eq!(shm.slot_header(i).unwrap().rx_count.load(Ordering::Relaxed), 0);
        }
    }

    #[test]
    fn cleanup_stale_nonexistent() {
        let result = cleanup_stale_queue("test/queue_shm_nonexist_xyz");
        assert_eq!(result.unwrap(), false);
    }
}
