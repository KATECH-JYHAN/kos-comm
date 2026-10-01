// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Jun-young Han <jyhan@katech.re.kr>
// KATECH SDV Platform Research Center

use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::sync::{Mutex, OnceLock};

use crate::error::{Error, Result};

struct Inner {
    map: HashMap<String, FdEntry>,
}

#[derive(Clone, Copy)]
pub struct FdEntry {
    pub fd: RawFd,
    pub slot_count: u32,
    pub slot_size: u32,
}

pub struct FdRegistry {
    inner: Mutex<Inner>,
}

static INSTANCE: OnceLock<FdRegistry> = OnceLock::new();

impl FdRegistry {
    fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                map: HashMap::new(),
            }),
        }
    }

    pub fn global() -> &'static FdRegistry {
        INSTANCE.get_or_init(FdRegistry::new)
    }

    pub fn register(&self, topic: &str, fd: RawFd, slot_count: u32, slot_size: u32) {
        let mut inner = self.inner.lock().unwrap();
        inner.map.insert(
            topic.to_string(),
            FdEntry {
                fd,
                slot_count,
                slot_size,
            },
        );
    }

    pub fn lookup(&self, topic: &str) -> Option<FdEntry> {
        let inner = self.inner.lock().unwrap();
        inner.map.get(topic).copied()
    }

    pub fn unregister(&self, topic: &str) -> Option<FdEntry> {
        let mut inner = self.inner.lock().unwrap();
        inner.map.remove(topic)
    }

    pub fn topics(&self) -> Vec<String> {
        let inner = self.inner.lock().unwrap();
        inner.map.keys().cloned().collect()
    }
}

pub fn send_fd(sock_fd: RawFd, fd_to_send: RawFd) -> Result<()> {
    use std::io::IoSlice;

    let payload = [b'F'];
    let iov = [IoSlice::new(&payload)];

    let cmsg_space = unsafe { libc::CMSG_SPACE(std::mem::size_of::<RawFd>() as u32) } as usize;
    let mut cmsg_buf = vec![0u8; cmsg_space];

    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = iov.as_ptr() as *mut libc::iovec;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
    msg.msg_controllen = cmsg_space;

    let cmsg = unsafe { libc::CMSG_FIRSTHDR(&msg) };
    if cmsg.is_null() {
        return Err(Error::ShmCreateFailed);
    }
    unsafe {
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type = libc::SCM_RIGHTS;
        (*cmsg).cmsg_len = libc::CMSG_LEN(std::mem::size_of::<RawFd>() as u32) as _;
        std::ptr::copy_nonoverlapping(
            &fd_to_send as *const RawFd as *const u8,
            libc::CMSG_DATA(cmsg),
            std::mem::size_of::<RawFd>(),
        );
    }

    let n = unsafe { libc::sendmsg(sock_fd, &msg, 0) };
    if n < 0 {
        Err(Error::ShmCreateFailed)
    } else {
        Ok(())
    }
}

pub fn recv_fd(sock_fd: RawFd) -> Result<RawFd> {
    use std::io::IoSliceMut;

    let mut payload = [0u8; 1];
    let mut iov = [IoSliceMut::new(&mut payload)];

    let cmsg_space = unsafe { libc::CMSG_SPACE(std::mem::size_of::<RawFd>() as u32) } as usize;
    let mut cmsg_buf = vec![0u8; cmsg_space];

    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = iov.as_mut_ptr() as *mut libc::iovec;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
    msg.msg_controllen = cmsg_space;

    let n = unsafe { libc::recvmsg(sock_fd, &mut msg, 0) };
    if n <= 0 {
        return Err(Error::ShmAttachFailed);
    }

    let cmsg = unsafe { libc::CMSG_FIRSTHDR(&msg) };
    if cmsg.is_null() {
        return Err(Error::ShmAttachFailed);
    }

    unsafe {
        if (*cmsg).cmsg_level != libc::SOL_SOCKET || (*cmsg).cmsg_type != libc::SCM_RIGHTS {
            return Err(Error::ShmAttachFailed);
        }
        let mut fd: RawFd = -1;
        std::ptr::copy_nonoverlapping(
            libc::CMSG_DATA(cmsg),
            &mut fd as *mut RawFd as *mut u8,
            std::mem::size_of::<RawFd>(),
        );
        if fd < 0 {
            Err(Error::ShmAttachFailed)
        } else {
            Ok(fd)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_register_lookup_unregister() {
        let reg = FdRegistry::new();
        reg.register("test/topic", 42, 4, 256);

        let entry = reg.lookup("test/topic").unwrap();
        assert_eq!(entry.fd, 42);
        assert_eq!(entry.slot_count, 4);
        assert_eq!(entry.slot_size, 256);

        assert!(reg.lookup("nonexistent").is_none());

        reg.unregister("test/topic");
        assert!(reg.lookup("test/topic").is_none());
    }

    #[test]
    fn registry_topics_list() {
        let reg = FdRegistry::new();
        reg.register("a/b", 1, 4, 128);
        reg.register("c/d", 2, 4, 128);
        let mut topics = reg.topics();
        topics.sort();
        assert_eq!(topics, vec!["a/b", "c/d"]);
    }

    #[test]
    fn send_recv_fd_via_socketpair() {
        let mut fds = [0i32; 2];
        let rc = unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
        assert_eq!(rc, 0);

        let null_fd = unsafe { libc::open(b"/dev/null\0".as_ptr() as *const _, libc::O_RDONLY) };
        assert!(null_fd >= 0);

        send_fd(fds[0], null_fd).unwrap();

        let received = recv_fd(fds[1]).unwrap();
        assert!(received >= 0);
        assert_ne!(received, null_fd);

        unsafe {
            libc::close(null_fd);
            libc::close(received);
            libc::close(fds[0]);
            libc::close(fds[1]);
        }
    }
}
