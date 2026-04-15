#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

pub mod error;
pub mod logger;
pub mod msg;
pub mod status;
pub mod status_web;
pub mod util;

pub use error::InternalError;
