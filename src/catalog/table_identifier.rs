/*!
Defining the [TableIdentifier] struct for identifying tables in an iceberg catalog.
*/

use core::fmt::{self, Display};

use super::namespace::Namespace;
use anyhow::{anyhow, Result};

/// Seperator of different namespace levels.
pub static SEPARATOR: &str = ".";

///Identifies a table in an iceberg catalog.
#[derive(Clone)]
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
                "Error: Cannot create a TableIdentifier from an empty sequence.",
            ))
        } else if names[length - 1].is_empty() {
            Err(anyhow!("Error: Table name cannot be empty.",))
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
            .split(SEPARATOR)
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
        write!(f, "{}{}{}", self.namespace, SEPARATOR, self.name)
    }
}

#[cfg(test)]

mod tests {
    use super::TableIdentifier;

    #[test]
    fn test_new() {
        let identifier = TableIdentifier::try_new(&vec![
            "level1".to_string(),
            "level2".to_string(),
            "table".to_string(),
        ])
        .unwrap();
        assert_eq!(&format!("{}", identifier), "level1.level2.table");
    }
    #[test]
    #[should_panic]
    fn test_empty() {
        let _ = TableIdentifier::try_new(&vec![
            "level1".to_string(),
            "level2".to_string(),
            "".to_string(),
        ])
        .unwrap();
    }
    #[test]
    #[should_panic]
    fn test_empty_identifier() {
        let _ = TableIdentifier::try_new(&vec![]).unwrap();
    }
    #[test]
    fn test_parse() {
        let identifier = TableIdentifier::parse("level1.level2.table").unwrap();
        assert_eq!(&format!("{}", identifier), "level1.level2.table");
    }
}
