/*!
 * Types in iceberg
 */
use crate::model::decimal::Decimal;

/// Values present in iceberg type
pub enum Value {
     /// 0x00 for false, non-zero byte for true
     Boolean(bool),
     /// Stored as 4-byte little-endian
     Int(i32),
     /// Stored as 8-byte little-endian
     Long(i64),
     /// Stored as 4-byte little-endian
     Double(f32),
     /// Stored as 8-byte little-endian
     Long(f64),
     /// Stores days from the 1970-01-01 in an 4-byte little-endian int
     Date(chrono::naive::Date),
     /// Stores microseconds from midnight in an 8-byte little-endian long
     Time(chrono::naive::Time),
     /// Stores microseconds from 1970-01-01 00:00:00.000000 in an 8-byte little-endian long
     Timestamp(chrono::naive::NaiveDateTime),
     /// Stores microseconds from 1970-01-01 00:00:00.000000 in an 8-byte little-endian long
     TimestampTZ(chrono::DateTime),
     /// UTF-8 bytes (without length) 
     String(String),
     /// 16-byte big-endian value
     UUID(uuid::UUID),
     /// Binary value
     Fixed(usize, Vec<u8>),
     /// Binary value (without length)
     Binary(Vec<u8>),
     /// Stores unscaled value as two’s-complement big-endian binary, 
     /// using the minimum number of bytes for the value
     Decimal(Decimal),
     
     /// struct?
     /// list?
     /// map?
}



#[cfg(test)]
mod tests {

}