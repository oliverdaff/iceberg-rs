use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
enum PrimativeTypes {
    /// True or False
    Bool,
    /// 32-bit signed integer
    Int,
    /// 64-bit signed integer
    Long,
    /// 32-bit IEEE 753 floating bit.
    Float,
    /// 64-bit IEEE 753 floating bit.
    Double,
    /// Fixed point decimal
    Decimal { precision: i32, scale: u8 },
    /// Calendar date without timezone or time.
    Date,
    /// Time of day without date or timezone.
    Time,
    /// Timestamp without timezone
    Timestamp,
    /// Timestamp with timezone
    Timestampz,
    /// Arbitrary-length character sequeces
    String,
    /// Universally Unique Identifiers
    Uuid,
    /// Fixed length byte array
    Fixed(usize),
    /// Arbitray-lenght byte array.
    Binary,
}
