/*!
 * Defines the different [Operation]s on a [Table].
*/

///Table operations
pub enum TableOperation {
    /// Update schema
    UpdateSchema,
    /// Update spec
    UpdateSpec,
    /// Update table properties
    UpdateProperties,
    /// Replace the sort order
    ReplaceSortOrder,
    /// Update the table location
    UpdateLocation,
    /// Append new files to the table
    NewAppend,
    /// Quickly append new files to the table
    NewFastAppend,
    /// Replace files in the table and commit
    NewRewrite,
    /// Replace manifests files and commit
    RewriteManifests,
    /// Replace files in the table by a filter expression
    NewOverwrite,
    /// Remove or replace rows in existing data files
    NewRowDelta,
    /// Delete files in the table and commit
    NewDelete,
    /// Expire snapshots in the table
    ExpireSnapshots,
    /// Manage snapshots in the table
    ManageSnapshots,
    /// Commit multiple table operations at once
    NewTransaction,
    /// Read and write table data and metadata files
    IO,
}
