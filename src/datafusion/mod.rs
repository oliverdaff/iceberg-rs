/*!
 * Tableprovider to use iceberg table with datafusion.
*/

use anyhow::Result;
use futures::TryStreamExt;
use std::{any::Any, collections::HashMap, ops::DerefMut, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    common::DataFusionError,
    datasource::{listing::PartitionedFile, object_store::ObjectStoreUrl, TableProvider},
    execution::context::SessionState,
    logical_expr::TableType,
    logical_plan::{combine_filters, Expr},
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::{file_format::FileScanConfig, ExecutionPlan},
    scalar::ScalarValue,
};
use url::Url;

use crate::{
    datafusion::pruning_statistics::{PruneDataFiles, PruneManifests},
    model::manifest::ManifestEntry,
    table::Table,
};

mod pruning_statistics;
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
        TableType::Base
    }
    async fn scan(
        &self,
        session: &SessionState,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let schema = self.schema();

        let object_store_url = ObjectStoreUrl::parse(&self.metadata().location)?;
        let url: &Url = object_store_url.as_ref();
        session.runtime_env.register_object_store(
            url.scheme(),
            url.host_str().unwrap_or_default(),
            self.0.object_store(),
        );

        let mut file_groups: HashMap<Vec<ScalarValue>, Vec<PartitionedFile>> = HashMap::new();
        if let Some(Some(predicate)) = (!filters.is_empty()).then_some(combine_filters(filters)) {
            let pruning_predicate = PruningPredicate::try_new(predicate, schema.clone())?;
            let manifests_to_prune = pruning_predicate.prune(&PruneManifests::from(self))?;
            let files = self
                .files(Some(manifests_to_prune))
                .await
                .map_err(|err| DataFusionError::Internal(format!("{}", err)))?
                .try_collect::<Vec<_>>()
                .await
                .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;
            let files_to_prune = pruning_predicate.prune(&PruneDataFiles::new(self, &files))?;
            files
                .into_iter()
                .zip(files_to_prune.into_iter())
                .for_each(|(manifest, prune_file)| {
                    if !prune_file {
                        // let part = partitioned_file_from_action(action, &schema);
                        // file_groups
                        //     .entry(part.partition_values.clone())
                        //     .or_default()
                        //     .push(part);
                    };
                });
        } else {
            // self.get_state().files().iter().for_each(|action| {
            //     let part = partitioned_file_from_action(action, &schema);
            //     file_groups
            //         .entry(part.partition_values.clone())
            //         .or_default()
            //         .push(part);
            // });
        };

        // let file_scan_config = FileScanConfig { object_store_url };
        unimplemented!()
    }
}
