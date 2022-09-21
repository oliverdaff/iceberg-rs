/*!
 * Tableprovider to use iceberg table with datafusion.
*/

use std::{any::Any, ops::DerefMut, pin::Pin, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    common::DataFusionError,
    datasource::TableProvider,
    execution::context::SessionState,
    logical_expr::{TableProviderFilterPushDown, TableType},
    logical_plan::Expr,
    physical_plan::ExecutionPlan,
};
use futures::Future;

use crate::table::Table;

mod schema;

/// Iceberg table for datafusion
pub struct DataFusionTable(Table);

impl core::ops::Deref for DataFusionTable {
    type Target = Table;

    fn deref(self: &'_ DataFusionTable) -> &'_ Self::Target {
        &self.0
    }
}

impl DerefMut for DataFusionTable {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Table> for DataFusionTable {
    fn from(value: Table) -> Self {
        DataFusionTable(value)
    }
}

#[async_trait::async_trait]
impl TableProvider for DataFusionTable {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        Arc::new(self.0.schema().try_into().unwrap())
    }
    fn table_type(&self) -> TableType {
        unimplemented!()
    }
    fn scan<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        ctx: &'life1 SessionState,
        projection: &'life2 Option<Vec<usize>>,
        filters: &'life3 [Expr],
        limit: Option<usize>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Arc<dyn ExecutionPlan>, DataFusionError>>
                + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        unimplemented!()
    }

    fn get_table_definition(&self) -> Option<&str> {
        unimplemented!()
    }
    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown, DataFusionError> {
        unimplemented!()
    }
}
