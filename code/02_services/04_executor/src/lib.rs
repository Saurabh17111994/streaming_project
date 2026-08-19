//! Nautilus execution service library crate.
//!
//! Replaces the retired Python executor scaffold (see `legacy_python/`) with a long-lived Rust
//! execution/position authority. See `../README.md` and the implementation plan.

pub mod bridge;
pub mod gate;
