//! # Iceberg-rs
//!
//! > ⚠️ **DEPRECATED**: This crate is no longer maintained.
//! > Please use the official [Apache Iceberg Rust implementation](https://crates.io/crates/iceberg) instead.
//!
//! ---
//!
//! `iceberg-rs` was an early Rust library for working with [Apache Iceberg](https://iceberg.apache.org/).
//! It provided data structures for serializing and deserializing Iceberg table metadata.
//!
//! **This crate is now deprecated.** We recommend migrating to the official implementation:
//!
//! - 📦 Crate: [`iceberg`](https://crates.io/crates/iceberg)
//! - 🔗 Repository: [apache/iceberg-rust](https://github.com/apache/iceberg-rust)
//!
//! The official crate offers comprehensive features, including:
//!
//! - Table operations: create, read, update, delete
//! - Schema evolution and hidden partitioning
//! - Time travel and snapshot isolation
//! - View and materialized view support
//! - Multiple catalog implementations: REST, AWS Glue, File-based
//! - Integration with Apache Arrow and DataFusion
//!
//! For more details, visit the [official documentation](https://docs.rs/iceberg/latest/iceberg/).
//!
//! ---
//!
//! Thank you to everyone who used and contributed to `iceberg-rs`.

pub mod model;
