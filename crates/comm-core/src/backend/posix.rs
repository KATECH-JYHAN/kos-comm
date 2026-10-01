// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Jun-young Han <jyhan@katech.re.kr>
// KATECH SDV Platform Research Center

use super::MappedRegion;
use crate::error::{Error, Result};
use std::ffi::CString;
use std::ptr;

#[cfg(not(target_os = "android"))]
pub fn create_mapping(shm_name: &str, size: usize) -> Result<MappedRegion> {
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

    if unsafe { libc::ftruncate(fd, size as libc::off_t) } != 0 {
        unsafe {
            libc::close(fd);
            libc::shm_unlink(c_name.as_ptr());
        }
        return Err(Error::ShmCreateFailed);
    }

    let addr = unsafe {
        libc::mmap(
            ptr::null_mut(),
            size,
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

    Ok(MappedRegion {
        ptr: addr as *mut u8,
        size,
        fd,
        needs_unmap: true,
    })
}

#[cfg(not(target_os = "android"))]
pub fn attach_mapping(shm_name: &str, size: usize, readonly: bool) -> Result<MappedRegion> {
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

    let prot = if readonly {
        libc::PROT_READ
    } else {
        libc::PROT_READ | libc::PROT_WRITE
    };

    let addr = unsafe { libc::mmap(ptr::null_mut(), size, prot, libc::MAP_SHARED, fd, 0) };

    if addr == libc::MAP_FAILED {
        unsafe { libc::close(fd) };
        return Err(Error::ShmAttachFailed);
    }

    Ok(MappedRegion {
        ptr: addr as *mut u8,
        size,
        fd,
        needs_unmap: true,
    })
}

#[cfg(not(target_os = "android"))]
pub fn unlink(shm_name: &str) {
    if let Ok(c_name) = CString::new(shm_name) {
        unsafe {
            libc::shm_unlink(c_name.as_ptr());
        }
    }
}
