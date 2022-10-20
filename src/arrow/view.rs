/*!
 * Tableprovider to use iceberg table with datafusion.
*/

use std::ops::DerefMut;

use anyhow::Result;

use datafusion::{datasource::ViewTable, prelude::SessionContext};

use crate::{model::view_metadata::Representation, view::View};

/// Iceberg table for datafusion
pub struct DataFusionView(View);

impl core::ops::Deref for DataFusionView {
    type Target = View;

    fn deref(self: &'_ DataFusionView) -> &'_ Self::Target {
        &self.0
    }
}

impl DerefMut for DataFusionView {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<View> for DataFusionView {
    fn from(value: View) -> Self {
        DataFusionView(value)
    }
}

impl DataFusionView {
    /// Get DataFusion View Table that implements TableProvider.
    pub fn to_view_table(&self, ctx: &SessionContext) -> Result<ViewTable> {
        match self.metadata().representation() {
            Representation::Sql { sql, .. } => {
                let logical_plan = ctx.create_logical_plan(sql)?;
                ViewTable::try_new(logical_plan, Some(sql.to_string()))
            }
        }
        .map_err(anyhow::Error::msg)
    }
}
