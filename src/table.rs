use serde::{de::Error, Deserialize, Deserializer, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
enum TableMetadataFormatVersion {
    #[serde(rename = "1")]
    V1,
    #[serde(rename = "2")]
    V2,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", remote = "Self")]
struct TableMetadataV2 {
    format_version: TableMetadataFormatVersion,

    table_uuid: Uuid,
}

impl<'de> Deserialize<'de> for TableMetadataV2 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let this = Self::deserialize(deserializer)?;

        if !matches!(this.format_version, TableMetadataFormatVersion::V2) {
            return Err(D::Error::custom("format-version should be 2"));
        }

        Ok(this)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::TableMetadataV2;

    #[test]
    fn test_deserialize_table_data_v2() -> Result<()> {
        let data = r#"
            {
                "format-version" : "2",
                "table-uuid": "550e8400-e29b-41d4-a716-446655440000"
            }
        "#;
        let metadata = serde_json::from_str::<TableMetadataV2>(&data)?;
        assert!(matches!(
            metadata.format_version,
            crate::table::TableMetadataFormatVersion::V2
        ));
        Ok(())
    }

    #[test]
    fn test_invalid_table_uuid() -> Result<()> {
        let data = r#"
            {
                "format-version" : "2",
                "table-uuid": "xxxx"
            }
        "#;
        assert!(serde_json::from_str::<TableMetadataV2>(&data).is_err());
        Ok(())
    }
    #[test]
    fn test_deserialize_table_data_v2_invalid_format_version() -> Result<()> {
        let data = r#"
            {
                "format-version" : "1"
            }
        "#;
        assert!(serde_json::from_str::<TableMetadataV2>(&data).is_err());
        Ok(())
    }
}
