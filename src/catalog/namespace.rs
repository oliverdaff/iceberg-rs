/*!
Defining the [Namespace] struct for handling namespaces in the catalog.
*/

use crate::error::{IcebergError, Result};

/// Namespace struct for iceberg catalogs
pub struct Namespace {
    levels: Vec<String>,
}

impl Namespace {
    /// Try to create new namespace with sequence of strings.
    pub fn try_new(levels: &[String]) -> Result<Self> {
        if levels.iter().any(|x| x.is_empty()) {
            Err(IcebergError::Message(
                "Error: Cannot create a namespace with an empty entry.".to_string(),
            ))
        } else {
            Ok(Namespace {
                levels: levels.to_vec(),
            })
        }
    }
    /// Create empty namespace
    pub fn empty() -> Self {
        Namespace { levels: vec![] }
    }
    /// Get the namespace levels
    pub fn levels(&self) -> &[String] {
        &self.levels
    }
    /// Get the number of levels
    pub fn len(&self) -> usize {
        self.levels.len()
    }
}
