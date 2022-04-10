use lazy_static::lazy_static;
use regex::Regex;
use serde::{
    de::{self, IntoDeserializer},
    Deserialize, Deserializer, Serialize,
};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "lowercase")]
#[serde(remote = "Self")]
enum PartitionTransform {
    /// Always produces `null`
    Void,
    /// Source value, unmodified
    Identity,
    /// Extract a date or timestamp year as years from 1970
    Year,
    /// Extract a date or timestamp month as months from 1970-01-01
    Month,
    /// Extract a date or timestamp day as days from 1970-01-01
    Day,
    /// Extract a date or timestamp hour as hours from 1970-01-01 00:00:00
    Hour,
    /// Hash of value, mod N
    Bucket(u32),
    /// Value truncated to width
    Truncate(u32),
}

impl<'de> Deserialize<'de> for PartitionTransform {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s.starts_with("bucket") {
            deserialize_bucket(s.into_deserializer())
        } else if s.starts_with("truncate") {
            deserialize_truncate(s.into_deserializer())
        } else {
            PartitionTransform::deserialize(s.into_deserializer())
        }
    }
}

impl Serialize for PartitionTransform {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use PartitionTransform::*;
        match self {
            Bucket(mod_n) => serializer.serialize_str(&format!("bucket[{mod_n}]")),
            Truncate(width) => serializer.serialize_str(&format!("truncate[{width}]")),
            _ => PartitionTransform::serialize(&self, serializer),
        }
    }
}

fn deserialize_bucket<'de, D>(deserializer: D) -> Result<PartitionTransform, D::Error>
where
    D: Deserializer<'de>,
{
    let this = String::deserialize(deserializer)?;
    lazy_static! {
        static ref RE: Regex = Regex::new(r#"^bucket\[(?P<n>\d+)\]$"#).unwrap();
    }
    let err_msg = format!("Invalid bucket format {}", this);

    let caps = RE
        .captures(&this)
        .ok_or_else(|| de::Error::custom(&err_msg))?;
    let bucket: u32 = caps
        .name("n")
        .ok_or_else(|| de::Error::custom(&err_msg))
        .and_then(|p| {
            p.as_str()
                .parse()
                .map_err(|_| de::Error::custom("bucket not u32"))
        })?;
    Ok(PartitionTransform::Bucket(bucket))
}

fn deserialize_truncate<'de, D>(deserializer: D) -> Result<PartitionTransform, D::Error>
where
    D: Deserializer<'de>,
{
    let this = String::deserialize(deserializer)?;
    lazy_static! {
        static ref RE: Regex = Regex::new(r#"^truncate\[(?P<w>\d+)\]$"#).unwrap();
    }
    let err_msg = format!("Invalid truncate format {}", this);

    let caps = RE
        .captures(&this)
        .ok_or_else(|| de::Error::custom(&err_msg))?;
    let width: u32 = caps
        .name("w")
        .ok_or_else(|| de::Error::custom(&err_msg))
        .and_then(|p| {
            p.as_str()
                .parse()
                .map_err(|_| de::Error::custom("bucket not u32"))
        })?;
    Ok(PartitionTransform::Truncate(width))
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionField {
    /// A source column id from the table’s schema
    source_id: i32,
    /// A partition field id that is used to identify a partition field and is unique within a partition spec.
    /// In v2 table metadata, it is unique across all partition specs.
    field_id: i32,
    /// A partition name.
    name: String,
    /// A transform that is applied to the source column to produce a partition value.
    transform: PartitionTransform,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_field() {
        let data = r#"
            {  
                "source-id": 4,  
                "field-id": 1000,  
                "name": "ts_day",  
                "transform": "day"
            } 
        "#;
        let partition_field: PartitionField = serde_json::from_str(data).unwrap();

        assert_eq!(4, partition_field.source_id);
        assert_eq!(1000, partition_field.field_id);
        assert_eq!("ts_day", partition_field.name);
        assert_eq!(PartitionTransform::Day, partition_field.transform);
    }

    #[test]
    fn test_all_transforms() {
        let transforms = vec![
            PartitionTransform::Void,
            PartitionTransform::Identity,
            PartitionTransform::Year,
            PartitionTransform::Month,
            PartitionTransform::Day,
            PartitionTransform::Hour,
            PartitionTransform::Bucket(10),
            PartitionTransform::Truncate(10),
        ];
        for transform in transforms {
            let field = PartitionField {
                source_id: 4,
                field_id: 1000,
                name: "ts_day".to_string(),
                transform: transform.clone(),
            };
            let json = serde_json::to_string(&field).unwrap();
            let partition_field: PartitionField = serde_json::from_str(&json).unwrap();

            assert_eq!(4, partition_field.source_id);
            assert_eq!(1000, partition_field.field_id);
            assert_eq!("ts_day", partition_field.name);
            assert_eq!(transform, partition_field.transform);
        }
    }
}