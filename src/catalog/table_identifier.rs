/*!
Defining the [TableIdentifier] struct for identifying tables in an iceberg catalog.
*/

use super::namespace::Namespace;
use anyhow::{anyhow, Result};

///Identifies a table in an iceberg catalog.
pub struct TableIdentifier {
    namespace: Namespace,
    name: String,
}

impl TableIdentifier {
    ///Create TableIdentifier
    pub fn try_new(names: &[String]) -> Result<Self> {
        let length = names.len();
        if names.is_empty() {
            Err(anyhow!(
                "Error: Cannot create a TableIdentifier from an empty sequence."
            ))
        } else if names[length].is_empty() {
            Err(anyhow!("Error: Table name cannot be empty."))
        } else {
            Ok(TableIdentifier {
                namespace: Namespace::try_new(&names[0..length - 1])?,
                name: names[length].clone(),
            })
        }
    }
    ///Parse
    pub fn parse(identifier: &str) -> Result<Self> {
        let names = identifier
            .split('.')
            .map(|x| x.to_string())
            .collect::<Vec<String>>();
        TableIdentifier::try_new(&names)
    }
}
