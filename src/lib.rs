#![deny(missing_docs)]
/*!
# Iceberg-rs

Iceberg-rs is a library for working with [Apache Iceberg](https://iceberg.apache.org/).

The Iceberg-rs [model] package consists of data structures that know how to
serialise and deserialise the Iceberg table format.

Currently supported:
* Parsing table metadata v2.

Coming soon:
* Manifest files.
* Manifest lists.
* v1 table metadata support.
* Validation.

*/
pub mod catalog;
pub mod model;
pub mod table;
pub mod transaction;

pub use object_store;
