/*!
Defines traits to communicate with an iceberg catalog.
*/

mod namespace;
pub mod table_builder;
pub mod table_identifier;

use crate::model::schema::SchemaV2;

use table_builder::TableBuilder;
use table_identifier::TableIdentifier;
///Trait to create, replace and drop tables in an iceberg catalog.
pub trait Catalog {
    ///Instantiate a builder to either create a table or start a create/replace transaction.
    fn build_table(identifier: TableIdentifier, schema: SchemaV2) -> Box<dyn TableBuilder>;
}
