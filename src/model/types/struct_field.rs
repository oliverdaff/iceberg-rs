/*!
 * Struct type in iceberg
 */

use super::Nullable;

/// Struct field is tuple of name, value, and optional documentation.
pub type StructField = (String, Nullable, Option<String>);
