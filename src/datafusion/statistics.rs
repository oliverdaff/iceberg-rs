use datafusion::physical_plan::{ColumnStatistics, Statistics};

use super::DataFusionTable;
use anyhow::Result;

impl DataFusionTable {
    pub(crate) async fn statistics(&self) -> Result<Statistics> {
        self.manifests().iter().fold(
            Ok(Statistics {
                num_rows: Some(0),
                total_byte_size: None,
                column_statistics: Some(vec![
                    ColumnStatistics {
                        null_count: None,
                        max_value: None,
                        min_value: None,
                        distinct_count: None
                    };
                    self.schema().fields.len()
                ]),
                is_exact: true,
            }),
            |acc, x| {
                let acc = acc?;
                Ok(Statistics {
                    num_rows: acc
                        .num_rows
                        .zip(x.added_files_count())
                        .map(|(num_rows, added_files_count)| num_rows + added_files_count as usize),
                    total_byte_size: None,
                    column_statistics: Some(vec![
                        ColumnStatistics {
                            null_count: None,
                            max_value: None,
                            min_value: None,
                            distinct_count: None
                        };
                        self.schema().fields.len()
                    ]),
                    is_exact: true,
                })
            },
        )
    }
}
