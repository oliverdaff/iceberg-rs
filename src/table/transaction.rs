/*!
 * Defines the [Transaction] type that performs multiple [TableOperation]s with ACID properties.
*/

use crate::{error::IcebergError, error::Result, model::schema::SchemaV2};

use super::{operation::Operation, Table};

pub struct Transaction<'table> {
    table: &'table mut Table,
    operations: Vec<Operation>,
}

impl<'table> Transaction<'table> {
    pub fn new(table: &'table mut Table) -> Self {
        Transaction {
            table: table,
            operations: vec![],
        }
    }
    pub fn update_schema(&mut self, schema: SchemaV2) {
        self.operations.push(Operation::UpdateSchema(schema))
    }
    pub fn update_spec(&mut self, spec_id: i32) {
        self.operations.push(Operation::UpdateSpec(spec_id))
    }
    pub async fn commit(self) -> Result<()> {
        Err(IcebergError::Message("Not implemented.".to_string()))
    }
}
