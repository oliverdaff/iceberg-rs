use serde::{de::Error, Deserialize, Deserializer, Serialize};

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
                "format-version" : "2"
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
    fn test_deserialize_table_data_v2_invalid() -> Result<()> {
        let data = r#"
            {
                "format-version" : "1"
            }
        "#;
        matches!(serde_json::from_str::<TableMetadataV2>(&data), Err(_));
        Ok(())
    }
}
