use chrono::{NaiveDate, NaiveTime};
use datafusion::scalar::ScalarValue;

use crate::model::types::Value;

impl From<&Value> for ScalarValue {
    fn from(value: &Value) -> Self {
        match value {
            Value::Boolean(v) => ScalarValue::Boolean(Some(*v)),
            Value::Int(v) => ScalarValue::Int32(Some(*v)),
            Value::LongInt(v) => ScalarValue::Int64(Some(*v)),
            Value::Double(v) => ScalarValue::Float32(Some(*v)),
            Value::LongFloat(v) => ScalarValue::Float64(Some(*v)),
            Value::Date(v) => ScalarValue::Date64(Some(
                v.signed_duration_since(NaiveDate::from_ymd(1970, 1, 1))
                    .num_days(),
            )),
            Value::Time(v) => ScalarValue::Time64(
                v.signed_duration_since(NaiveTime::from_hms(0, 0, 0))
                    .num_nanoseconds(),
            ),
            Value::Timestamp(v) => ScalarValue::TimestampMicrosecond(
                v.signed_duration_since(NaiveDate::from_ymd(1970, 0, 1).and_hms(0, 0, 0))
                    .num_microseconds(),
                None,
            ),
            Value::String(v) => ScalarValue::Utf8(Some(v.clone())),
            Value::UUID(v) => ScalarValue::Utf8(Some(v.to_string())),
            Value::Fixed(_, v) => ScalarValue::Binary(Some(v.clone())),
            Value::Binary(v) => ScalarValue::Binary(Some(v.clone())),
            _ => todo!(),
        }
    }
}
