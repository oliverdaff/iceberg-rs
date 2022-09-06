/*!
 * Types in iceberg
 */

use self::{decimal::Decimal, struct_field::StructField};

pub mod decimal;
pub mod struct_field;

/// Values present in iceberg type
pub enum Value {
    /// 0x00 for false, non-zero byte for true
    Boolean(bool),
    /// Stored as 4-byte little-endian
    Int(i32),
    /// Stored as 8-byte little-endian
    LongInt(i64),
    /// Stored as 4-byte little-endian
    Double(f32),
    /// Stored as 8-byte little-endian
    LongFloat(f64),
    /// Stores days from the 1970-01-01 in an 4-byte little-endian int
    Date(chrono::NaiveDate),
    /// Stores microseconds from midnight in an 8-byte little-endian long
    Time(chrono::NaiveTime),
    /// Stores microseconds from 1970-01-01 00:00:00.000000 in an 8-byte little-endian long
    Timestamp(chrono::naive::NaiveDateTime),
    /// Stores microseconds from 1970-01-01 00:00:00.000000 in an 8-byte little-endian long
    TimestampTZ(chrono::NaiveDateTime),
    /// UTF-8 bytes (without length)
    String(String),
    /// 16-byte big-endian value
    UUID(uuid::Uuid),
    /// Binary value
    Fixed(usize, Vec<u8>),
    /// Binary value (without length)
    Binary(Vec<u8>),
    /// Stores unscaled value as two’s-complement big-endian binary,
    /// using the minimum number of bytes for the value
    Decimal(Decimal),
    /// A struct is a tuple of typed values. Each field in the tuple is named and has an integer id that is unique in the table schema.
    /// Each field can be either optional or required, meaning that values can (or cannot) be null. Fields may be any type.
    /// Fields may have an optional comment or doc string. Fields can have default values.
    Struct(Vec<StructField>),
    // list?
    // map?
}

/// Nullable Value
pub enum Nullable {
    /// Required value
    Required(Value),
    /// Optional value, can be null
    Optional(Option<Value>),
}

#[cfg(test)]
mod tests {}
