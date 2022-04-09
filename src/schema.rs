use lazy_static::lazy_static;
use regex::Regex;
use serde::{
    de::{self, IntoDeserializer},
    Deserialize, Deserializer, Serialize,
};

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[serde(remote = "Self")]
/// Primiative Types within a schemam.
pub enum PrimitiveType {
    /// True or False
    Boolean,
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
    Fixed(u64),
    /// Arbitray-lenght byte array.
    Binary,
}

/// Serialize for PrimitiveType wit special handling for
/// Decimal and Fixedt types.
impl Serialize for PrimitiveType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use PrimitiveType::*;
        match self {
            Decimal {
                precision: p,
                scale: s,
            } => serializer.serialize_str(&format!("decimal({p},{s})")),
            Fixed(l) => serializer.serialize_str(&format!("fixed[{l}]")),
            _ => PrimitiveType::serialize(&self, serializer),
        }
    }
}

/// Serialize for PrimitiveType wit special handling for
/// Decimal and Fixedt types.
impl<'de> Deserialize<'de> for PrimitiveType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s.starts_with("decimal") {
            deserialize_decimal(s.into_deserializer())
        } else if s.starts_with("fixed") {
            deserialize_fixed(s.into_deserializer())
        } else {
            PrimitiveType::deserialize(s.into_deserializer())
        }
    }
}

/// Parsing for the Decimal PrimitiveType
fn deserialize_decimal<'de, D>(deserializer: D) -> Result<PrimitiveType, D::Error>
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
    Ok(PrimitiveType::Decimal { precision, scale })
}

/// Deserialize for the Fixed PrimitiveType
fn deserialize_fixed<'de, D>(deserializer: D) -> Result<PrimitiveType, D::Error>
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
    Ok(PrimitiveType::Fixed(length))
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
/// Type for struct
#[serde(rename_all = "lowercase")]
enum StructNestedType {
    Struct,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
/// Type for List
enum ListNestedType {
    List,
}
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
/// Type for Map
enum MapNestedType {
    Map,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
/// A struct is a tuple of typed values. Each field in the tuple is
/// named and has an integer id that is unique in the table schema.
/// Each field can be either optional or required, meaning that values can (or cannot) be null.
/// Fields may be any type.
/// Fields may have an optional comment or doc string.
struct Struct {
    #[serde(alias = "type")]
    struct_type: StructNestedType,

    //
    fields: Vec<StructField>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StructField {
    /// Unique Id
    id: i32,
    /// Field Name
    name: String,
    /// Optional or required, meaning that values can (or can not be null)
    required: bool,
    // Field can have any type
    field_type: PrimitiveType,
    /// Fields can have any optional comment or doc string.
    doc: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct Schema {
    /// Identifier of the schema
    schema_id: i32,
    /// Set of primitive fields that identify rows in a table.
    identifier_field_ids: Option<Vec<i32>>,

    /// Always the string "struct".
    #[serde(alias = "type")]
    struct_type: StructNestedType,

    /// List of fields.
    fields: Vec<StructField>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_struct_type() {
        let data = r#"
        {
            "type" : "struct",
            "fields": []
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
            PrimitiveType::Decimal {
                precision: 1,
                scale: 1
            }
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
    fn test_boolean() {
        let data = r#"
        {
            "id" : 1,
            "name": "struct_name",
            "required": true,
            "field_type": "boolean"
        }
        "#;
        let result_struct = serde_json::from_str::<StructField>(data).unwrap();
        assert!(matches!(result_struct.field_type, PrimitiveType::Boolean));
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
        assert!(matches!(result_struct.field_type, PrimitiveType::Fixed(1),));

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

    #[test]
    fn test_all_valid_types() {
        let type_mappings = vec![
            PrimitiveType::Boolean,
            PrimitiveType::Int,
            PrimitiveType::Long,
            PrimitiveType::Float,
            PrimitiveType::Double,
            PrimitiveType::Decimal {
                precision: 1,
                scale: 2,
            },
            PrimitiveType::Date,
            PrimitiveType::Time,
            PrimitiveType::Timestamp,
            PrimitiveType::Timestampz,
            PrimitiveType::String,
            PrimitiveType::Uuid,
            PrimitiveType::Fixed(1),
            PrimitiveType::Binary,
        ];

        for primitive in type_mappings {
            let sf = StructField {
                id: 1,
                name: "name".to_string(),
                required: true,
                field_type: primitive.clone(),
                doc: None,
            };

            let j = serde_json::to_string(&sf).unwrap();
            let unserde: StructField = serde_json::from_str(&j).unwrap();
            assert_eq!(unserde.field_type, primitive);
        }
    }

    #[test]
    fn test_schema() {
        let data = r#"
        {
            "schema-id" : 1,
            "type": "struct",
            "fields" : [
                {   
                    "id" : 1,
                    "name": "struct_name",
                    "required": true,
                    "field_type": "fixed[1]"
                }
            ]
        }
        "#;
        let result_struct = serde_json::from_str::<Schema>(data).unwrap();
        assert_eq!(1, result_struct.schema_id);
        assert_eq!(None, result_struct.identifier_field_ids);
        assert_eq!(1, result_struct.fields.len());
    }
}
