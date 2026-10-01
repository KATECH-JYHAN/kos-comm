// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Jun-young Han <jyhan@katech.re.kr>
// KATECH SDV Platform Research Center

use super::MappedRegion;
use crate::error::{Error, Result};
use core::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::ptr;

pub const IVSHMEM_MAGIC: u32 = 0x4956_5348;

pub const IVSHMEM_VERSION: u32 = 1;

pub const MAX_ALLOCS: usize = 128;

const SUPER_HEADER_SIZE: usize = core::mem::size_of::<IvshmemSuperHeader>();

const ALLOC_ENTRY_SIZE: usize = core::mem::size_of::<AllocEntry>();

const DATA_REGION_OFFSET: usize = SUPER_HEADER_SIZE + MAX_ALLOCS * ALLOC_ENTRY_SIZE;

pub const DEFAULT_BAR_SIZE: usize = 16 * 1024 * 1024;

#[repr(C, align(64))]
pub struct IvshmemSuperHeader {
    pub magic: AtomicU32,
    pub version: AtomicU32,
    pub total_size: AtomicU32,
    pub num_allocations: AtomicU32,
    pub next_free_offset: AtomicU32,
    pub alloc_lock: AtomicU32,
    _pad: [u8; 232],
}

const _: () = assert!(core::mem::size_of::<IvshmemSuperHeader>() == 256);

#[repr(C, align(64))]
pub struct AllocEntry {
    pub topic_hash: AtomicU32,
    pub ecu_id: AtomicU32,
    pub flags: AtomicU32,
    pub offset: AtomicU32,
    pub size: AtomicU32,
    pub heartbeat_ns: AtomicU64,
    _pad: [u8; 32],
}

const _: () = assert!(core::mem::size_of::<AllocEntry>() == 64);

const FLAG_ACTIVE: u32 = 1;

#[derive(Debug, Clone)]
pub struct IvshmemConfig {
    pub device_path: String,
    pub ecu_id: u16,
    pub bar_size: usize,
}

static mut BAR_BASE: *mut u8 = ptr::null_mut();
static mut BAR_SIZE: usize = 0;
static mut BAR_FD: libc::c_int = -1;
static INIT_ONCE: AtomicU32 = AtomicU32::new(0);

fn fnv1a_hash(topic: &str) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for &b in topic.as_bytes() {
        h ^= b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

const SPINLOCK_MAX_SPINS: u32 = 1_000_000;

#[inline]
fn spinlock_acquire(lock: &AtomicU32) -> Result<()> {
    for _ in 0..SPINLOCK_MAX_SPINS {
        match lock.compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed) {
            Ok(_) => return Ok(()),
            Err(_) => core::hint::spin_loop(),
        }
    }
    Err(Error::Timeout)
}

#[inline]
fn spinlock_release(lock: &AtomicU32) {
    lock.store(0, Ordering::Release);
}

pub fn init(config: &IvshmemConfig) -> Result<()> {
    if INIT_ONCE.load(Ordering::Acquire) == 1 {
        return Ok(());
    }

    let bar_size = if config.bar_size > 0 {
        config.bar_size
    } else {
        DEFAULT_BAR_SIZE
    };

    let c_path =
        std::ffi::CString::new(config.device_path.as_str()).map_err(|_| Error::IvshmemOpenFailed)?;

    let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_RDWR | libc::O_SYNC) };
    if fd < 0 {
        return Err(Error::IvshmemOpenFailed);
    }

    let addr = unsafe {
        libc::mmap(
            ptr::null_mut(),
            bar_size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd,
            0,
        )
    };

    if addr == libc::MAP_FAILED {
        unsafe { libc::close(fd) };
        return Err(Error::IvshmemMapFailed);
    }

    unsafe {
        BAR_BASE = addr as *mut u8;
        BAR_SIZE = bar_size;
        BAR_FD = fd;
    }

    let hdr = unsafe { &*(addr as *const IvshmemSuperHeader) };
    if hdr.magic.load(Ordering::Acquire) != IVSHMEM_MAGIC {
        hdr.version.store(IVSHMEM_VERSION, Ordering::Release);
        hdr.total_size.store(bar_size as u32, Ordering::Release);
        hdr.num_allocations.store(0, Ordering::Release);
        hdr.next_free_offset
            .store(DATA_REGION_OFFSET as u32, Ordering::Release);
        hdr.alloc_lock.store(0, Ordering::Release);
        hdr.magic.store(IVSHMEM_MAGIC, Ordering::Release);
    } else {
        if hdr.version.load(Ordering::Acquire) != IVSHMEM_VERSION {
            return Err(Error::IvshmemNotInitialized);
        }
    }

    INIT_ONCE.store(1, Ordering::Release);
    Ok(())
}

pub unsafe fn init_from_raw(base: *mut u8, size: usize, fd: libc::c_int) -> Result<()> {
    BAR_BASE = base;
    BAR_SIZE = size;
    BAR_FD = fd;

    let hdr = &*(base as *const IvshmemSuperHeader);
    if hdr.magic.load(Ordering::Acquire) != IVSHMEM_MAGIC {
        hdr.version.store(IVSHMEM_VERSION, Ordering::Release);
        hdr.total_size.store(size as u32, Ordering::Release);
        hdr.num_allocations.store(0, Ordering::Release);
        hdr.next_free_offset
            .store(DATA_REGION_OFFSET as u32, Ordering::Release);
        hdr.alloc_lock.store(0, Ordering::Release);
        hdr.magic.store(IVSHMEM_MAGIC, Ordering::Release);
    }

    INIT_ONCE.store(1, Ordering::Release);
    Ok(())
}

pub unsafe fn reset_global_state() {
    if !BAR_BASE.is_null() && BAR_SIZE > 0 {
        libc::munmap(BAR_BASE as *mut libc::c_void, BAR_SIZE);
    }
    if BAR_FD >= 0 {
        libc::close(BAR_FD);
    }
    BAR_BASE = ptr::null_mut();
    BAR_SIZE = 0;
    BAR_FD = -1;
    INIT_ONCE.store(0, Ordering::Release);
}

pub fn is_initialized() -> bool {
    INIT_ONCE.load(Ordering::Acquire) == 1
}

fn super_header() -> Result<&'static IvshmemSuperHeader> {
    if INIT_ONCE.load(Ordering::Acquire) != 1 {
        return Err(Error::IvshmemNotInitialized);
    }
    unsafe { Ok(&*(BAR_BASE as *const IvshmemSuperHeader)) }
}

fn alloc_table() -> Result<&'static [AllocEntry; MAX_ALLOCS]> {
    if INIT_ONCE.load(Ordering::Acquire) != 1 {
        return Err(Error::IvshmemNotInitialized);
    }
    unsafe {
        let table_ptr = BAR_BASE.add(SUPER_HEADER_SIZE) as *const [AllocEntry; MAX_ALLOCS];
        Ok(&*table_ptr)
    }
}

fn align_up(val: usize, align: usize) -> usize {
    (val + align - 1) & !(align - 1)
}

pub fn create_mapping(topic: &str, size: usize, ecu_id: u16) -> Result<MappedRegion> {
    let hdr = super_header()?;
    let table = alloc_table()?;
    let topic_hash = fnv1a_hash(topic);

    let alloc_size = align_up(size, 64);

    spinlock_acquire(&hdr.alloc_lock)?;

    let num = hdr.num_allocations.load(Ordering::Acquire) as usize;
    for entry in &table[..num] {
        if entry.flags.load(Ordering::Acquire) & FLAG_ACTIVE != 0
            && entry.topic_hash.load(Ordering::Acquire) == topic_hash
        {
            spinlock_release(&hdr.alloc_lock);
            return Err(Error::ShmCreateFailed);
        }
    }

    if num >= MAX_ALLOCS {
        spinlock_release(&hdr.alloc_lock);
        return Err(Error::IvshmemFull);
    }

    let offset = hdr.next_free_offset.load(Ordering::Acquire) as usize;
    let bar_size = unsafe { BAR_SIZE };
    if offset + alloc_size > bar_size {
        spinlock_release(&hdr.alloc_lock);
        return Err(Error::IvshmemFull);
    }

    hdr.next_free_offset
        .store((offset + alloc_size) as u32, Ordering::Release);

    table[num].topic_hash.store(topic_hash, Ordering::Release);
    table[num].ecu_id.store(ecu_id as u32, Ordering::Release);
    table[num].offset.store(offset as u32, Ordering::Release);
    table[num].size.store(alloc_size as u32, Ordering::Release);
    table[num].heartbeat_ns.store(0, Ordering::Release);
    table[num].flags.store(FLAG_ACTIVE, Ordering::Release);

    hdr.num_allocations.store((num + 1) as u32, Ordering::Release);

    spinlock_release(&hdr.alloc_lock);

    let ptr = unsafe { BAR_BASE.add(offset) };

    Ok(MappedRegion {
        ptr,
        size: alloc_size,
        fd: unsafe { BAR_FD },
        needs_unmap: false,
    })
}

pub fn attach_mapping(topic: &str, _size: usize) -> Result<MappedRegion> {
    let table = alloc_table()?;
    let topic_hash = fnv1a_hash(topic);

    let hdr = super_header()?;
    let num = hdr.num_allocations.load(Ordering::Acquire) as usize;

    for entry in &table[..num] {
        if entry.flags.load(Ordering::Acquire) & FLAG_ACTIVE != 0
            && entry.topic_hash.load(Ordering::Acquire) == topic_hash
        {
            let offset = entry.offset.load(Ordering::Acquire) as usize;
            let alloc_size = entry.size.load(Ordering::Acquire) as usize;
            let ptr = unsafe { BAR_BASE.add(offset) };

            return Ok(MappedRegion {
                ptr,
                size: alloc_size,
                fd: unsafe { BAR_FD },
                needs_unmap: false,
            });
        }
    }

    Err(Error::ShmAttachFailed)
}

pub fn deactivate_topic(topic: &str) {
    let topic_hash = fnv1a_hash(topic);
    if let Ok(table) = alloc_table() {
        if let Ok(hdr) = super_header() {
            let num = hdr.num_allocations.load(Ordering::Acquire) as usize;
            for entry in &table[..num] {
                if entry.topic_hash.load(Ordering::Acquire) == topic_hash {
                    entry
                        .flags
                        .fetch_and(!FLAG_ACTIVE, Ordering::Release);
                    return;
                }
            }
        }
    }
}

pub fn update_heartbeat(topic: &str, heartbeat_ns: u64) {
    let topic_hash = fnv1a_hash(topic);
    if let Ok(table) = alloc_table() {
        if let Ok(hdr) = super_header() {
            let num = hdr.num_allocations.load(Ordering::Acquire) as usize;
            for entry in &table[..num] {
                if entry.flags.load(Ordering::Acquire) & FLAG_ACTIVE != 0
                    && entry.topic_hash.load(Ordering::Acquire) == topic_hash
                {
                    entry.heartbeat_ns.store(heartbeat_ns, Ordering::Release);
                    return;
                }
            }
        }
    }
}

pub fn cleanup_stale_heartbeat(timeout_ns: u64, now_ns: u64) -> usize {
    let table = match alloc_table() {
        Ok(t) => t,
        Err(_) => return 0,
    };
    let hdr = match super_header() {
        Ok(h) => h,
        Err(_) => return 0,
    };

    let num = hdr.num_allocations.load(Ordering::Acquire) as usize;
    let mut count = 0;

    for entry in &table[..num] {
        if entry.flags.load(Ordering::Acquire) & FLAG_ACTIVE == 0 {
            continue;
        }
        let hb = entry.heartbeat_ns.load(Ordering::Acquire);
        if hb > 0 && now_ns.saturating_sub(hb) > timeout_ns {
            entry
                .flags
                .fetch_and(!FLAG_ACTIVE, Ordering::Release);
            count += 1;
        }
    }

    count
}

pub fn dump_allocations() -> Vec<AllocInfo> {
    let mut result = Vec::new();

    let table = match alloc_table() {
        Ok(t) => t,
        Err(_) => return result,
    };
    let hdr = match super_header() {
        Ok(h) => h,
        Err(_) => return result,
    };

    let num = hdr.num_allocations.load(Ordering::Acquire) as usize;

    for (i, entry) in table[..num].iter().enumerate() {
        result.push(AllocInfo {
            index: i,
            topic_hash: entry.topic_hash.load(Ordering::Acquire),
            ecu_id: entry.ecu_id.load(Ordering::Acquire) as u16,
            active: entry.flags.load(Ordering::Acquire) & FLAG_ACTIVE != 0,
            offset: entry.offset.load(Ordering::Acquire),
            size: entry.size.load(Ordering::Acquire),
            heartbeat_ns: entry.heartbeat_ns.load(Ordering::Acquire),
        });
    }

    result
}

#[derive(Debug, Clone)]
pub struct AllocInfo {
    pub index: usize,
    pub topic_hash: u32,
    pub ecu_id: u16,
    pub active: bool,
    pub offset: u32,
    pub size: u32,
    pub heartbeat_ns: u64,
}

pub fn bar_info() -> Result<BarInfo> {
    let hdr = super_header()?;
    Ok(BarInfo {
        magic: hdr.magic.load(Ordering::Acquire),
        version: hdr.version.load(Ordering::Acquire),
        total_size: hdr.total_size.load(Ordering::Acquire),
        num_allocations: hdr.num_allocations.load(Ordering::Acquire),
        next_free_offset: hdr.next_free_offset.load(Ordering::Acquire),
    })
}

#[derive(Debug, Clone)]
pub struct BarInfo {
    pub magic: u32,
    pub version: u32,
    pub total_size: u32,
    pub num_allocations: u32,
    pub next_free_offset: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static TEST_LOCK: Mutex<()> = Mutex::new(());

    unsafe fn create_test_bar(size: usize) -> (*mut u8, libc::c_int) {
        let name = std::ffi::CString::new("ivshmem_test").unwrap();
        let fd = libc::syscall(libc::SYS_memfd_create, name.as_ptr(), 0 as libc::c_uint) as libc::c_int;
        assert!(fd >= 0, "memfd_create failed");
        assert_eq!(libc::ftruncate(fd, size as libc::off_t), 0);
        let addr = libc::mmap(
            ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd,
            0,
        );
        assert_ne!(addr, libc::MAP_FAILED);
        ptr::write_bytes(addr as *mut u8, 0, size);
        (addr as *mut u8, fd)
    }

    unsafe fn destroy_test_bar(base: *mut u8, size: usize, fd: libc::c_int) {
        libc::munmap(base as *mut libc::c_void, size);
        libc::close(fd);
    }

    fn run_locked<F: FnOnce()>(f: F) {
        let _guard = TEST_LOCK.lock().unwrap();
        f();
    }

    #[test]
    fn super_header_init_and_validate() {
        run_locked(|| {
            let size = 1024 * 1024;
            unsafe {
                reset_global_state();
                let (base, fd) = create_test_bar(size);
                init_from_raw(base, size, fd).unwrap();

                let hdr = super_header().unwrap();
                assert_eq!(hdr.magic.load(Ordering::Acquire), IVSHMEM_MAGIC);
                assert_eq!(hdr.version.load(Ordering::Acquire), IVSHMEM_VERSION);
                assert_eq!(hdr.total_size.load(Ordering::Acquire), size as u32);
                assert_eq!(hdr.num_allocations.load(Ordering::Acquire), 0);
                assert_eq!(
                    hdr.next_free_offset.load(Ordering::Acquire),
                    DATA_REGION_OFFSET as u32
                );

                BAR_BASE = ptr::null_mut();
                BAR_SIZE = 0;
                BAR_FD = -1;
                INIT_ONCE.store(0, Ordering::Release);
                destroy_test_bar(base, size, fd);
            }
        });
    }

    #[test]
    fn allocate_and_attach() {
        run_locked(|| {
            let size = 1024 * 1024;
            unsafe {
                reset_global_state();
                let (base, fd) = create_test_bar(size);
                init_from_raw(base, size, fd).unwrap();

                let region_a = create_mapping("/sensor/lidar", 4096, 1).unwrap();
                assert!(!region_a.ptr.is_null());
                assert!(region_a.size >= 4096);

                let region_b = create_mapping("/sensor/camera", 2048, 1).unwrap();
                assert_ne!(region_a.ptr, region_b.ptr);

                let attach_a = attach_mapping("/sensor/lidar", 4096).unwrap();
                assert_eq!(attach_a.ptr, region_a.ptr);

                let result = attach_mapping("/nonexistent", 4096);
                assert!(result.is_err());

                let hdr = super_header().unwrap();
                assert_eq!(hdr.num_allocations.load(Ordering::Acquire), 2);

                BAR_BASE = ptr::null_mut();
                BAR_SIZE = 0;
                BAR_FD = -1;
                INIT_ONCE.store(0, Ordering::Release);
                destroy_test_bar(base, size, fd);
            }
        });
    }

    #[test]
    fn duplicate_topic_rejected() {
        run_locked(|| {
            let size = 1024 * 1024;
            unsafe {
                reset_global_state();
                let (base, fd) = create_test_bar(size);
                init_from_raw(base, size, fd).unwrap();

                create_mapping("/topic/dup", 1024, 1).unwrap();
                let result = create_mapping("/topic/dup", 1024, 2);
                assert!(result.is_err());

                BAR_BASE = ptr::null_mut();
                BAR_SIZE = 0;
                BAR_FD = -1;
                INIT_ONCE.store(0, Ordering::Release);
                destroy_test_bar(base, size, fd);
            }
        });
    }

    #[test]
    fn bar_full_error() {
        run_locked(|| {
            let size = DATA_REGION_OFFSET + 128;
            unsafe {
                reset_global_state();
                let (base, fd) = create_test_bar(size);
                init_from_raw(base, size, fd).unwrap();

                create_mapping("/topic/big", 64, 1).unwrap();
                let result = create_mapping("/topic/too_big", 128, 1);
                assert_eq!(result.unwrap_err(), Error::IvshmemFull);

                BAR_BASE = ptr::null_mut();
                BAR_SIZE = 0;
                BAR_FD = -1;
                INIT_ONCE.store(0, Ordering::Release);
                destroy_test_bar(base, size, fd);
            }
        });
    }

    #[test]
    fn deactivate_and_heartbeat() {
        run_locked(|| {
            let size = 1024 * 1024;
            unsafe {
                reset_global_state();
                let (base, fd) = create_test_bar(size);
                init_from_raw(base, size, fd).unwrap();

                create_mapping("/topic/hb", 1024, 1).unwrap();
                update_heartbeat("/topic/hb", 1_000_000_000);

                let allocs = dump_allocations();
                assert_eq!(allocs.len(), 1);
                assert!(allocs[0].active);
                assert_eq!(allocs[0].heartbeat_ns, 1_000_000_000);

                deactivate_topic("/topic/hb");
                let allocs = dump_allocations();
                assert!(!allocs[0].active);

                BAR_BASE = ptr::null_mut();
                BAR_SIZE = 0;
                BAR_FD = -1;
                INIT_ONCE.store(0, Ordering::Release);
                destroy_test_bar(base, size, fd);
            }
        });
    }

    #[test]
    fn stale_heartbeat_cleanup() {
        run_locked(|| {
            let size = 1024 * 1024;
            unsafe {
                reset_global_state();
                let (base, fd) = create_test_bar(size);
                init_from_raw(base, size, fd).unwrap();

                create_mapping("/topic/stale", 1024, 1).unwrap();
                update_heartbeat("/topic/stale", 1_000_000_000);

                let cleaned = cleanup_stale_heartbeat(3_000_000_000, 10_000_000_000);
                assert_eq!(cleaned, 1);

                let allocs = dump_allocations();
                assert!(!allocs[0].active);

                BAR_BASE = ptr::null_mut();
                BAR_SIZE = 0;
                BAR_FD = -1;
                INIT_ONCE.store(0, Ordering::Release);
                destroy_test_bar(base, size, fd);
            }
        });
    }

    #[test]
    fn bar_info_query() {
        run_locked(|| {
            let size = 1024 * 1024;
            unsafe {
                reset_global_state();
                let (base, fd) = create_test_bar(size);
                init_from_raw(base, size, fd).unwrap();

                let info = bar_info().unwrap();
                assert_eq!(info.magic, IVSHMEM_MAGIC);
                assert_eq!(info.version, IVSHMEM_VERSION);
                assert_eq!(info.total_size, size as u32);
                assert_eq!(info.num_allocations, 0);

                BAR_BASE = ptr::null_mut();
                BAR_SIZE = 0;
                BAR_FD = -1;
                INIT_ONCE.store(0, Ordering::Release);
                destroy_test_bar(base, size, fd);
            }
        });
    }

    #[test]
    fn not_initialized_error() {
        run_locked(|| {
            unsafe {
                reset_global_state();
            }
            assert_eq!(
                create_mapping("/topic/x", 1024, 1).unwrap_err(),
                Error::IvshmemNotInitialized
            );
            assert_eq!(
                attach_mapping("/topic/x", 1024).unwrap_err(),
                Error::IvshmemNotInitialized
            );
            assert_eq!(
                bar_info().unwrap_err(),
                Error::IvshmemNotInitialized
            );
        });
    }
}
