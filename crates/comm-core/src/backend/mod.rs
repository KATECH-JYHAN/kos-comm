// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Jun-young Han <jyhan@katech.re.kr>
// KATECH SDV Platform Research Center

pub mod posix;

#[cfg(feature = "ivshmem")]
pub mod ivshmem;

#[derive(Debug)]
pub struct MappedRegion {
    pub ptr: *mut u8,
    pub size: usize,
    pub fd: libc::c_int,
    pub needs_unmap: bool,
}

unsafe impl Send for MappedRegion {}
unsafe impl Sync for MappedRegion {}
