/*!
 * Defines the different [Operation]s on a [View].
*/

use anyhow::Result;

use crate::{model::schema::Schema, view::View};

/// View operation
pub enum Operation {
    /// Update schema
    UpdateSchema(Schema),
    // /// Update table properties
    // UpdateProperties,
    // /// Update the table location
    // UpdateLocation,
}

impl Operation {
    /// Execute operation
    pub async fn execute(self, view: &mut View) -> Result<()> {
        match self {
            _ => Ok(()),
        }
    }
}
