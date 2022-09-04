/*!
 * Defines the different [Operation]s on a [Table].
*/

use crate::model::schema::SchemaV2;

use super::Table;

///Table operations
pub enum Operation {
    /// Update schema
    UpdateSchema(SchemaV2),
    /// Update spec
    UpdateSpec(i32),
    // /// Update table properties
    // UpdateProperties,
    // /// Replace the sort order
    // ReplaceSortOrder,
    // /// Update the table location
    // UpdateLocation,
    // /// Append new files to the table
    // NewAppend,
    // /// Quickly append new files to the table
    // NewFastAppend,
    // /// Replace files in the table and commit
    // NewRewrite,
    // /// Replace manifests files and commit
    // RewriteManifests,
    // /// Replace files in the table by a filter expression
    // NewOverwrite,
    // /// Remove or replace rows in existing data files
    // NewRowDelta,
    // /// Delete files in the table and commit
    // NewDelete,
    // /// Expire snapshots in the table
    // ExpireSnapshots,
    // /// Manage snapshots in the table
    // ManageSnapshots,
    // /// Read and write table data and metadata files
    // IO,
}

impl Operation {
    pub fn execute(self, table: &Table) {
        let _ = table;
        unimplemented!()
    }
}
