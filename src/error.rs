/*!
 * defines the [IcebergError] and [IcebergResult] types.
*/

use thiserror::Error;

/// Iceberg erro type
#[derive(Error, Debug)]
pub enum IcebergError {
    /// General error that does not need to be handled and displays a message.
    #[error("{0}")]
    Message(String),
}

/// Iceberg result type
pub type Result<T> = std::result::Result<T, IcebergError>;
