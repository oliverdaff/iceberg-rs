/*!
 * Defines the [View] struct that represents an iceberg view.
*/

use crate::view_spec::view_metadata::ViewMetadata;

/// An iceberg view
pub struct View {
    /// Metadata for the iceberg view according to the iceberg view spec
    pub metadata: ViewMetadata,
    /// Path to the current metadata location
    pub metadata_location: String,
}
