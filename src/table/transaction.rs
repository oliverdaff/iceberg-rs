/*!
 * Defines the [Transaction] type that performs multiple [TableOperation]s with ACID properties.
*/

use super::operation::TableOperation;

pub struct Transaction(Vec<TableOperation>);

impl Transaction {
    pub fn commit() {}
}
