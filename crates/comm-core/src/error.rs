// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Jun-young Han <jyhan@katech.re.kr>
// KATECH SDV Platform Research Center

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Error {
    NoData = 1,
    Timeout = 2,
    TopicNotFound = 3,
    VersionMismatch = 4,
    AuthFailed = 5,
    BufferFull = 6,
    Stale = 7,
    Disconnected = 8,
    WritePos = 9,
    InvalidRange = 10,

    ShmCreateFailed = 11,
    ShmAttachFailed = 12,
    ShmValidationFailed = 13,
    AlreadyBorrowed = 14,
    NoBorrow = 15,
    InvalidConfig = 16,

    QueueFull = 17,
    DataIndexOutOfRange = 18,
    QueueValidationFailed = 19,
    DeadlineMissed = 20,

    IvshmemOpenFailed = 21,
    IvshmemMapFailed = 22,
    IvshmemFull = 23,
    IvshmemNotInitialized = 24,
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Error::NoData => write!(f, "no data available"),
            Error::Timeout => write!(f, "operation timed out"),
            Error::TopicNotFound => write!(f, "topic not found"),
            Error::VersionMismatch => write!(f, "schema version mismatch"),
            Error::AuthFailed => write!(f, "authentication failed"),
            Error::BufferFull => write!(f, "ring buffer full"),
            Error::Stale => write!(f, "reference was forcibly released"),
            Error::Disconnected => write!(f, "daemon connection lost"),
            Error::WritePos => write!(f, "attempted to read write-position slot"),
            Error::InvalidRange => write!(f, "invalid time range"),
            Error::ShmCreateFailed => write!(f, "SHM create failed"),
            Error::ShmAttachFailed => write!(f, "SHM attach failed"),
            Error::ShmValidationFailed => write!(f, "SHM validation failed"),
            Error::AlreadyBorrowed => write!(f, "publisher already holds a borrow"),
            Error::NoBorrow => write!(f, "no active borrow"),
            Error::InvalidConfig => write!(f, "invalid configuration"),
            Error::QueueFull => write!(f, "queue full"),
            Error::DataIndexOutOfRange => write!(f, "data index out of range"),
            Error::QueueValidationFailed => write!(f, "queue validation failed"),
            Error::DeadlineMissed => write!(f, "deadline missed"),
            Error::IvshmemOpenFailed => write!(f, "IVSHMEM device open failed"),
            Error::IvshmemMapFailed => write!(f, "IVSHMEM BAR mmap failed"),
            Error::IvshmemFull => write!(f, "IVSHMEM allocation table full"),
            Error::IvshmemNotInitialized => write!(f, "IVSHMEM backend not initialized"),
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = core::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_repr_values() {
        assert_eq!(Error::NoData as u8, 1);
        assert_eq!(Error::InvalidRange as u8, 10);
        assert_eq!(Error::ShmCreateFailed as u8, 11);
        assert_eq!(Error::NoBorrow as u8, 15);
    }

    #[test]
    fn error_display() {
        assert_eq!(format!("{}", Error::BufferFull), "ring buffer full");
    }
}
