use lazy_static::lazy_static;
use regex::Regex;
use serde::{de, Deserialize, Deserializer, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged, rename_all = "lowercase")]
/// Primiative Types within a schemam.
enum PrimativeType {
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
    /// TODO: Create this in spark and see what it looks like in the schema
    Decimal(DecimalType),
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
    Fixed(FixedType),
    /// Arbitray-lenght byte array.
    Binary,
}

#[derive(Debug, Serialize)]
struct DecimalType {
    precision: i32,
    scale: u8,
}

impl<'de> Deserialize<'de> for DecimalType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let this = String::deserialize(deserializer)?;
        lazy_static! {
            static ref RE: Regex = Regex::new(r#"^decimal\((?P<p>\d+),(?P<s>\d+)\)$"#).unwrap();
        }

        let err_msg = format!("Invalid decimal format {}", this);

        let caps = RE.captures(&this).ok_or(de::Error::custom(&err_msg))?;
        let precision: i32 = caps
            .name("p")
            .ok_or(de::Error::custom(&err_msg))
            .and_then(|p| {
                p.as_str()
                    .parse()
                    .map_err(|_| de::Error::custom("precision not i32"))
            })?;
        let scale: u8 = caps
            .name("s")
            .ok_or(de::Error::custom(&err_msg))
            .and_then(|p| {
                p.as_str()
                    .parse()
                    .map_err(|_| de::Error::custom("scale not u8"))
            })?;
        Ok(DecimalType { precision, scale })
    }
}

#[derive(Debug, Serialize)]
struct FixedType(u64);

impl<'de> Deserialize<'de> for FixedType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let this = String::deserialize(deserializer)?;
        lazy_static! {
            static ref RE: Regex = Regex::new(r#"^fixed\[(?P<l>\d+)\]$"#).unwrap();
        }

        let err_msg = format!("Invalid fixed format {}", this);

        let caps = RE.captures(&this).ok_or(de::Error::custom(&err_msg))?;
        let length: u64 = caps
            .name("l")
            .ok_or(de::Error::custom(&err_msg))
            .and_then(|p| {
                p.as_str()
                    .parse()
                    .map_err(|_| de::Error::custom("length not u64"))
            })?;
        Ok(FixedType(length))
    }
}

#[derive(Debug, Serialize, Deserialize)]
/// Type for struct
#[serde(rename_all = "lowercase")]
enum StructNestedType {
    Struct,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
/// Type for List
enum ListNestedType {
    List,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
/// Type for Map
enum MapNestedType {
    Map,
}

#[derive(Debug, Serialize, Deserialize)]
/// A struct is a tuple of typed values. Each field in the tuple is
/// named and has an integer id that is unique in the table schema.
/// Each field can be either optional or required, meaning that values can (or cannot) be null.
/// Fields may be any type.
/// Fields may have an optional comment or doc string.
struct Struct {
    #[serde(alias = "type")]
    struct_type: StructNestedType,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StructField {
    /// Unique Id
    id: i32,
    /// Field Name
    name: String,
    /// Optional or required, meaning that values can (or can not be null)
    required: bool,
    // Field can have any type
    field_type: PrimativeType,
    /// Fields can have any optional comment or doc string.
    doc: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_struct_type() {
        let data = r#"
        {
            "type" : "struct"            
        }
        "#;
        assert!(serde_json::from_str::<Struct>(&data).is_ok());
        let data = r#"
        {
            "type" : "anyother"            
        }
        "#;
        assert!(serde_json::from_str::<Struct>(data).is_err());
    }

    #[test]
    fn test_decimal() {
        let data = r#"
        {
            "id" : 1,
            "name": "struct_name",
            "required": true,
            "field_type": "decimal(1,1)"
        }
        "#;
        let result_struct = serde_json::from_str::<StructField>(data).unwrap();
        assert!(matches!(
            result_struct.field_type,
            PrimativeType::Decimal(DecimalType {
                precision: 1,
                scale: 1
            })
        ));

        let invalid_decimal_data = r#"
        {
            "id" : 1,
            "name": "struct_name",
            "required": true,
            "field_type": "decimal(1,1000)"
        }
        "#;
        assert!(serde_json::from_str::<StructField>(invalid_decimal_data).is_err());
    }

    #[test]
    fn test_fixed() {
        let data = r#"
        {
            "id" : 1,
            "name": "struct_name",
            "required": true,
            "field_type": "fixed[1]"
        }
        "#;
        let result_struct = serde_json::from_str::<StructField>(data).unwrap();
        assert!(matches!(
            result_struct.field_type,
            PrimativeType::Fixed(FixedType(1))
        ));

        let invalid_fixed_data = r#"
        {
            "id" : 1,
            "name": "struct_name",
            "required": true,
            "field_type": "fixed[0.1]"
        }
        "#;
        assert!(serde_json::from_str::<StructField>(invalid_fixed_data).is_err());
    }
}
