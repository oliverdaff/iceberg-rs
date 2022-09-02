/*!
Defining the [TableIdentifier] struct for identifying tables in an iceberg catalog.
*/

use core::fmt::{self, Display};

use super::namespace::Namespace;
use crate::error::{IcebergError, Result};

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
            Err(IcebergError::Message(
                "Error: Cannot create a TableIdentifier from an empty sequence.".to_string(),
            ))
        } else if names[length - 1].is_empty() {
            Err(IcebergError::Message(
                "Error: Table name cannot be empty.".to_string(),
            ))
        } else {
            Ok(TableIdentifier {
                namespace: Namespace::try_new(&names[0..length - 1])?,
                name: names[length - 1].clone(),
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
    /// Return namespace of table
    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }
    /// Return name of table
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Display for TableIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.namespace, self.name)
    }
}
