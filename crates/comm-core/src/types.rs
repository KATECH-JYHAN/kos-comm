// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Jun-young Han <jyhan@katech.re.kr>
// KATECH SDV Platform Research Center

use core::mem;
use core::ptr;

#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct VarArray<T: Copy, const MAX: usize> {
    pub count: u32,
    pub data: [T; MAX],
}

impl<T: Copy + Default, const MAX: usize> Default for VarArray<T, MAX> {
    fn default() -> Self {
        Self {
            count: 0,
            data: [T::default(); MAX],
        }
    }
}

impl<T: Copy, const MAX: usize> VarArray<T, MAX> {
    fn read_count(&self) -> u32 {
        unsafe { ptr::addr_of!(self.count).read_unaligned() }
    }

    pub fn push(&mut self, value: T) -> bool {
        let c = self.read_count() as usize;
        if c >= MAX {
            return false;
        }
        unsafe {
            let elem_ptr = ptr::addr_of_mut!(self.data) as *mut T;
            elem_ptr.add(c).write_unaligned(value);
        }
        self.count = (c + 1) as u32;
        true
    }

    pub fn clear(&mut self) {
        self.count = 0;
    }

    pub fn len(&self) -> usize {
        (self.read_count() as usize).min(MAX)
    }

    pub fn is_empty(&self) -> bool {
        self.read_count() == 0
    }

    pub fn get(&self, i: usize) -> Option<T> {
        if i >= self.len() {
            return None;
        }
        unsafe {
            let elem_ptr = ptr::addr_of!(self.data) as *const T;
            Some(elem_ptr.add(i).read_unaligned())
        }
    }

    pub fn to_vec(&self) -> Vec<T> {
        let n = self.len();
        let mut v = Vec::with_capacity(n);
        for i in 0..n {
            unsafe {
                let elem_ptr = ptr::addr_of!(self.data) as *const T;
                v.push(elem_ptr.add(i).read_unaligned());
            }
        }
        v
    }

    pub fn actual_size(&self) -> usize {
        mem::size_of::<u32>() + self.len() * mem::size_of::<T>()
    }
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct VarString<const MAX: usize> {
    pub length: u16,
    pub data: [u8; MAX],
}

impl<const MAX: usize> Default for VarString<MAX> {
    fn default() -> Self {
        Self {
            length: 0,
            data: [0u8; MAX],
        }
    }
}

impl<const MAX: usize> VarString<MAX> {
    fn read_length(&self) -> u16 {
        unsafe { ptr::addr_of!(self.length).read_unaligned() }
    }

    pub fn set(&mut self, s: &str) {
        let bytes = s.as_bytes();
        let copy_len = bytes.len().min(MAX);
        unsafe {
            let data_ptr = ptr::addr_of_mut!(self.data) as *mut u8;
            ptr::copy_nonoverlapping(bytes.as_ptr(), data_ptr, copy_len);
        }
        self.length = copy_len as u16;
    }

    pub fn as_str(&self) -> &str {
        let len = (self.read_length() as usize).min(MAX);
        unsafe {
            let data_ptr = ptr::addr_of!(self.data) as *const u8;
            let slice = core::slice::from_raw_parts(data_ptr, len);
            core::str::from_utf8(slice).unwrap_or("")
        }
    }

    pub fn actual_size(&self) -> usize {
        mem::size_of::<u16>() + (self.read_length() as usize).min(MAX)
    }
}

#[repr(C, packed)]
#[derive(Clone, Copy, Default)]
pub struct Point2D {
    pub x: f64,
    pub y: f64,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Default)]
pub struct Point3D {
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Default)]
pub struct Vector3D {
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct Quaternion {
    pub w: f64,
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

impl Default for Quaternion {
    fn default() -> Self {
        Self {
            w: 1.0,
            x: 0.0,
            y: 0.0,
            z: 0.0,
        }
    }
}

#[repr(C, packed)]
#[derive(Clone, Copy, Default)]
pub struct Pose3D {
    pub position: Point3D,
    pub orientation: Quaternion,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn var_array_push_overflow() {
        let mut a = VarArray::<u32, 3>::default();
        assert!(a.is_empty());
        assert!(a.push(10));
        assert!(a.push(20));
        assert!(a.push(30));
        assert!(!a.push(40));
        assert_eq!(a.len(), 3);
        let v = a.to_vec();
        assert_eq!(v, vec![10, 20, 30]);
    }

    #[test]
    fn var_array_get() {
        let mut a = VarArray::<u32, 4>::default();
        a.push(100);
        a.push(200);
        assert_eq!(a.get(0), Some(100));
        assert_eq!(a.get(1), Some(200));
        assert_eq!(a.get(2), None);
    }

    #[test]
    fn var_array_clear() {
        let mut a = VarArray::<u8, 4>::default();
        a.push(1);
        a.push(2);
        a.clear();
        assert_eq!(a.len(), 0);
        assert!(a.is_empty());
    }

    #[test]
    fn var_array_actual_size() {
        let mut a = VarArray::<u32, 8>::default();
        assert_eq!(a.actual_size(), 4);
        a.push(1);
        a.push(2);
        assert_eq!(a.actual_size(), 12);
    }

    #[test]
    fn var_string_set_and_as_str() {
        let mut s = VarString::<16>::default();
        s.set("hello");
        assert_eq!(s.as_str(), "hello");
        assert_eq!(s.actual_size(), 2 + 5);
    }

    #[test]
    fn var_string_truncation() {
        let mut s = VarString::<4>::default();
        s.set("toolong");
        assert_eq!(s.as_str(), "tool");
    }

    #[test]
    fn sizeof_geometry() {
        assert_eq!(mem::size_of::<Point2D>(), 16);
        assert_eq!(mem::size_of::<Point3D>(), 24);
        assert_eq!(mem::size_of::<Vector3D>(), 24);
        assert_eq!(mem::size_of::<Quaternion>(), 32);
        assert_eq!(mem::size_of::<Pose3D>(), 24 + 32);
    }
}
