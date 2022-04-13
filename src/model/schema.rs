/*!
A table’s schema is a list of named columns. All data types are either primitives or nested types, which are maps,
lists, or structs. A table schema is also a struct type.
*/
use lazy_static::lazy_static;
use regex::Regex;
use serde::{
    de::{self, IntoDeserializer},
    Deserialize, Deserializer, Serialize,
};

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[serde(remote = "Self")]
/// Primitive Types within a schema.
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
    Decimal {
        /// The number of digits in the number.
        precision: i32,
        /// The number of digits to the right of the decimal point.
        scale: u8,
    },
    /// Calendar date without timezone or time.
    Date,
    /// Time of day without date or timezone.
    Time,
    /// Timestamp without timezone
    Timestamp,
    /// Timestamp with timezone
    Timestampz,
    /// Arbitrary-length character sequences
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
            _ => PrimitiveType::serialize(self, serializer),
        }
    }
}

/// Serialize for PrimitiveType wit special handling for
/// Decimal and Fixed types.
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

    let caps = RE
        .captures(&this)
        .ok_or_else(|| de::Error::custom(&err_msg))?;
    let precision: i32 = caps
        .name("p")
        .ok_or_else(|| de::Error::custom(&err_msg))
        .and_then(|p| {
            p.as_str()
                .parse()
                .map_err(|_| de::Error::custom("precision not i32"))
        })?;
    let scale: u8 = caps
        .name("s")
        .ok_or_else(|| de::Error::custom(&err_msg))
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

    let caps = RE
        .captures(&this)
        .ok_or_else(|| de::Error::custom(&err_msg))?;
    let length: u64 = caps
        .name("l")
        .ok_or_else(|| de::Error::custom(&err_msg))
        .and_then(|p| {
            p.as_str()
                .parse()
                .map_err(|_| de::Error::custom("length not u64"))
        })?;
    Ok(PrimitiveType::Fixed(length))
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
/// A union type of all allowed Schema types.
pub enum AllType {
    /// All the primitive types
    Primitive(PrimitiveType),
    /// A Struct type
    Struct(Struct),
    /// A List type.
    List(List),
    /// A Map type
    Map(Map),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type")]
/// A struct is a tuple of typed values. Each field in the tuple is
/// named and has an integer id that is unique in the table schema.
/// Each field can be either optional or required, meaning that values can (or cannot) be null.
/// Fields may be any type.
/// Fields may have an optional comment or doc string.
pub struct Struct {
    fields: Vec<StructField>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
/// Details of a struct in a field.
pub struct StructField {
    /// Unique Id
    id: i32,
    /// Field Name
    name: String,
    /// Optional or required, meaning that values can (or can not be null)
    required: bool,
    // Field can have any type
    field_type: AllType,
    /// Fields can have any optional comment or doc string.
    doc: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Names and types of fields in a table.
pub struct Schema {
    /// Identifier of the schema
    schema_id: i32,
    /// Set of primitive fields that identify rows in a table.
    identifier_field_ids: Option<Vec<i32>>,

    /// Name Mapping
    name_mapping: Option<NameMappings>,

    #[serde(flatten)]
    struct_fields: Struct,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case", tag = "list")]
/// A Schema type that contains List  elements.
pub struct List {
    /// Unique identifier for the element
    element_id: i32,

    element_required: bool,

    element: Box<AllType>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case", tag = "type")]
/// A Schema type that contains Map elements.
/// A map is a collection of key-value pairs with a key type and a value type.
/// Both the key field and value field each have an integer id that is unique
/// in the table schema. Map keys are required and map values can be either
/// optional or required. Both map keys and map values may be any type,
/// including nested types.
pub struct Map {
    ///Unique key field id
    key_id: i32,
    ///Type of the map key
    key: Box<AllType>,
    ///Unique key for the value id
    value_id: i32,
    ///Indicates if the value is required.
    value_required: bool,
    ///Type of the value.
    value: Box<AllType>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
/// Tables may also define a property schema.name-mapping.default with a JSON name mapping containing a list of field mapping objects.
/// These mappings provide fallback field ids to be used when a data file does not contain field id information.
pub struct NameMappings {
    default: Vec<NameMapping>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Individual mapping within NameMappings.
pub struct NameMapping {
    /// An optional Iceberg field ID used when a field’s name is present in names
    field_id: Option<i32>,
    /// A required list of 0 or more names for a field.
    names: Vec<String>,
    /// An optional list of field mappings for child field of structs, maps, and lists.
    fields: Option<Vec<NameMapping>>,
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
            AllType::Primitive(PrimitiveType::Decimal {
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
        assert!(matches!(
            result_struct.field_type,
            AllType::Primitive(PrimitiveType::Boolean)
        ));
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
            AllType::Primitive(PrimitiveType::Fixed(1),)
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
                field_type: AllType::Primitive(primitive.clone()),
                doc: None,
            };

            let j = serde_json::to_string(&sf).unwrap();
            let unserde: StructField = serde_json::from_str(&j).unwrap();
            assert_eq!(unserde.field_type, AllType::Primitive(primitive));
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
            ],
            "name-mapping": {
                "default" : [
                    { 
                        "field-id": 4, 
                        "names": ["latitude", "lat"] 
                    }
                ]
            }
        }
        "#;
        let result_struct = serde_json::from_str::<Schema>(data).unwrap();
        assert_eq!(1, result_struct.schema_id);
        assert_eq!(None, result_struct.identifier_field_ids);
        assert_eq!(1, result_struct.struct_fields.fields.len());
        assert_eq!(1, result_struct.name_mapping.unwrap().default.len());
    }

    #[test]
    fn test_list_type() {
        let data = r#"
                {  
                    "type": "list",  
                    "element-id": 3,  
                    "element-required": true,  
                    "element": "string"
                }
        "#;
        let result_struct = serde_json::from_str::<List>(data);
        let result_struct = result_struct.unwrap();
        assert_eq!(3, result_struct.element_id);
        assert!(result_struct.element_required);
        assert_eq!(
            AllType::Primitive(PrimitiveType::String),
            *result_struct.element
        );
    }

    #[test]
    fn test_map_type() {
        let data = r#"
        {  
            "type": "map",
            "key-id": 4,
            "key": "string",
            "value-id": 5,
            "value-required": false,
            "value": "double"
        }
        "#;
        let result_struct = serde_json::from_str::<Map>(data);
        let result_struct = result_struct.unwrap();
        assert_eq!(4, result_struct.key_id);
        assert!(!result_struct.value_required);
        assert_eq!(
            AllType::Primitive(PrimitiveType::Double),
            *result_struct.value
        );
        assert_eq!(
            AllType::Primitive(PrimitiveType::String),
            *result_struct.key
        );
    }

    #[test]
    fn test_name_mapping() {
        let data = r#"
        { "field-id": 3, "names": ["location"], "fields": [
            { "field-id": 4, "names": ["latitude", "lat"] },
            { "field-id": 5, "names": ["longitude", "long"] }
        ] }        
        "#;

        let name_mapping: NameMapping = serde_json::from_str(data).unwrap();
        assert_eq!(Some(3), name_mapping.field_id);
        assert!(name_mapping.fields.is_some())
    }
}
