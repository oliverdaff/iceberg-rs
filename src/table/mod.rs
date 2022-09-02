/*!
Defining the [Table] struct that represents an iceberg table.
*/

use std::sync::Arc;

use crate::{catalog::Catalog, model::table::TableMetadataV2};

use self::transaction::Transaction;

mod operation;
pub(crate) mod transaction;

///Iceberg table
pub struct Table {
    pub(crate) catalog: Arc<dyn Catalog>,
    pub(crate) metadata: TableMetadataV2,
}

impl Table {
    /// Create a new transaction for this table
    pub fn new_transaction(&mut self) -> Transaction {
        Transaction::new(self)
    }
}
