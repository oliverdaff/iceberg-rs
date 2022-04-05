use serde::{Serialize, Deserialize};



#[derive(Debug, Serialize, Deserialize)]
struct TableMetaDataV2 {
    format_version: String
}


#[cfg(test)]
mod tests {
    use anyhow::Result;

    #[test]
    fn test_deserialize_table_data_v2() -> Result<()> {
        let data = r#"
            "format-version" = "1"
        "#;
        serde_json::from_str(&data)?;
        Ok(())
    }
}