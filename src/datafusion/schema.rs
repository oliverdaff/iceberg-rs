/*!
 * Convert between datafusion and iceberg schema
*/

use anyhow::{anyhow, Result};

use std::collections::HashMap;

use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};

use crate::model::schema::{AllType, PrimitiveType, SchemaStruct};

pub fn iceberg_to_arrow_schema(schema: &SchemaStruct) -> Result<ArrowSchema> {
    let fields = schema
        .fields
        .iter()
        .map(|field| {
            Ok(Field::new_dict(
                &field.name,
                (&field.field_type).try_into()?,
                !field.required,
                field.id as i64,
                false,
            ))
        })
        .collect::<Result<_, anyhow::Error>>()?;
    let metadata = HashMap::new();
    Ok(ArrowSchema { fields, metadata })
}

impl TryFrom<&AllType> for DataType {
    type Error = anyhow::Error;

    fn try_from(value: &AllType) -> Result<Self, Self::Error> {
        match value {
            AllType::Primitive(primitive) => match primitive {
                PrimitiveType::Boolean => Ok(DataType::Boolean),
                PrimitiveType::Int => Ok(DataType::Int32),
                PrimitiveType::Long => Ok(DataType::Int64),
                PrimitiveType::Float => Ok(DataType::Float32),
                PrimitiveType::Double => Ok(DataType::Float64),
                PrimitiveType::Decimal { precision, scale } => {
                    Ok(DataType::Decimal128(*precision as u8, *scale))
                }
                PrimitiveType::Date => Ok(DataType::Date64),
                PrimitiveType::Time => Ok(DataType::Time64(TimeUnit::Millisecond)),
                PrimitiveType::Timestamp => Ok(DataType::Timestamp(TimeUnit::Millisecond, None)),
                PrimitiveType::Timestampz => Ok(DataType::Timestamp(
                    TimeUnit::Millisecond,
                    Some("UTC".to_string()),
                )),
                PrimitiveType::String => Ok(DataType::Utf8),
                PrimitiveType::Uuid => Ok(DataType::Utf8),
                PrimitiveType::Fixed(len) => Ok(DataType::FixedSizeBinary(*len as i32)),
                PrimitiveType::Binary => Ok(DataType::Binary),
            },
            AllType::List(list) => Ok(DataType::List(Box::new(Field::new_dict(
                "",
                (&list.element as &AllType).try_into()?,
                !list.element_required,
                list.element_id as i64,
                false,
            )))),
            AllType::Struct(struc) => Ok(DataType::Struct(
                struc
                    .fields
                    .iter()
                    .map(|field| {
                        Ok(Field::new_dict(
                            &field.name,
                            (&field.field_type).try_into()?,
                            !field.required,
                            field.id as i64,
                            false,
                        ))
                    })
                    .collect::<Result<_, anyhow::Error>>()?,
            )),
            AllType::Map(map) => Ok(DataType::Map(
                Box::new(Field::new_dict(
                    "entries",
                    DataType::Struct(vec![
                        Field::new_dict(
                            "key",
                            (&map.key as &AllType).try_into()?,
                            false,
                            map.key_id as i64,
                            false,
                        ),
                        Field::new_dict(
                            "value",
                            (&map.value as &AllType).try_into()?,
                            !map.value_required,
                            map.value_id as i64,
                            false,
                        ),
                    ]),
                    false,
                    0,
                    false,
                )),
                false,
            )),
        }
    }
}

impl TryFrom<&DataType> for AllType {
    type Error = anyhow::Error;

    fn try_from(value: &DataType) -> Result<Self, Self::Error> {
        match value {
            DataType::Boolean => Ok(AllType::Primitive(PrimitiveType::Boolean)),
            DataType::Int32 => Ok(AllType::Primitive(PrimitiveType::Int)),
            DataType::Int64 => Ok(AllType::Primitive(PrimitiveType::Long)),
            DataType::Float32 => Ok(AllType::Primitive(PrimitiveType::Float)),
            DataType::Float64 => Ok(AllType::Primitive(PrimitiveType::Double)),
            DataType::Decimal128(precision, scale) => {
                Ok(AllType::Primitive(PrimitiveType::Decimal {
                    precision: *precision as i32,
                    scale: *scale,
                }))
            }
            DataType::Date64 => Ok(AllType::Primitive(PrimitiveType::Date)),
            DataType::Time64(_) => Ok(AllType::Primitive(PrimitiveType::Time)),
            DataType::Timestamp(_, _) => Ok(AllType::Primitive(PrimitiveType::Timestamp)),
            DataType::Utf8 => Ok(AllType::Primitive(PrimitiveType::String)),
            DataType::FixedSizeBinary(len) => {
                Ok(AllType::Primitive(PrimitiveType::Fixed(*len as u64)))
            }
            DataType::Binary => Ok(AllType::Primitive(PrimitiveType::Binary)),
            _ => Err(anyhow!("Other arrow datatypes not supported")),
        }
    }
}
