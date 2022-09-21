/*!
 * Helper for iterating over files in a table.
*/
use std::{
    io::Cursor,
    iter::Map,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use anyhow::{anyhow, Result};
use apache_avro::{types::Value, Reader};
use futures::{Future, Stream, TryFutureExt};
use object_store::{path::Path, ObjectStore};

use crate::model::{manifest::ManifestEntry, manifest_list::ManifestFile};

use super::Table;

impl Table {
    /// Get files associated to a table
    pub async fn files(&self) -> Result<impl Stream<Item = Result<ManifestEntry>>> {
        let snapshot = if let Some(snapshots) = &self.metadata().snapshots {
            Ok(snapshots.last().unwrap())
        } else {
            Err(anyhow!("No snapshots in this table."))
        }?;
        let object_store = self.object_store();
        let path: Path = snapshot.manifest_list.clone().into();
        {
            let bytes: Cursor<Vec<u8>> = Cursor::new(
                object_store
                    .get(&path)
                    .await
                    .map_err(anyhow::Error::msg)?
                    .bytes()
                    .await?
                    .into(),
            );
            let reader = apache_avro::Reader::new(bytes)?;
            let map = reader.map(
                avro_value_to_manifest_file
                    as fn(
                        Result<Value, apache_avro::Error>,
                    ) -> Result<ManifestFile, apache_avro::Error>,
            );
            Ok(ManifestStream {
                object_store: self.object_store(),
                manifest_list_iter: map,
                manifest_iter: None,
            })
        }
    }
}

fn avro_value_to_manifest_file(
    entry: Result<Value, apache_avro::Error>,
) -> Result<ManifestFile, apache_avro::Error> {
    entry.and_then(|value| apache_avro::from_value(&value))
}

fn avro_value_to_manifest_entry(
    entry: Result<Value, apache_avro::Error>,
) -> Result<ManifestEntry, anyhow::Error> {
    entry
        .map_err(anyhow::Error::msg)
        .and_then(|value| value.try_into())
}

/// Iterator over all files in a given snapshot
pub struct ManifestStream<'list, 'manifest> {
    object_store: Arc<dyn ObjectStore>,
    manifest_list_iter: Map<
        Reader<'list, Cursor<Vec<u8>>>,
        fn(Result<Value, apache_avro::Error>) -> Result<ManifestFile, apache_avro::Error>,
    >,
    manifest_iter: Option<
        Map<
            Reader<'manifest, Cursor<Vec<u8>>>,
            fn(Result<Value, apache_avro::Error>) -> Result<ManifestEntry, anyhow::Error>,
        >,
    >,
}

impl<'list, 'manifest> Stream for ManifestStream<'list, 'manifest> {
    type Item = Result<ManifestEntry>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let next = match &mut self.manifest_iter {
            Some(manifest) => manifest.next(),
            None => None,
        };
        match next {
            None => {
                let next = self.manifest_list_iter.next();
                match next {
                    Some(file) => match file {
                        Ok(file) => {
                            let object_store = Arc::clone(&self.object_store);
                            let path: Path = file.manifest_path.clone().into();
                            let result = object_store.get(&path).and_then(|file| file.bytes());
                            let temp = match Pin::as_mut(&mut Box::pin(result)).poll(cx) {
                                Poll::Ready(file) => {
                                    let bytes = Cursor::new(Vec::from(file?));
                                    let mut reader = apache_avro::Reader::new(bytes)?;
                                    let next = reader.next();
                                    self.manifest_iter = Some(reader.map(
                                        avro_value_to_manifest_entry
                                            as fn(
                                                Result<Value, apache_avro::Error>,
                                            )
                                                -> Result<ManifestEntry, anyhow::Error>,
                                    ));
                                    Poll::Ready(next.map(avro_value_to_manifest_entry))
                                }
                                Poll::Pending => Poll::Pending,
                            };
                            temp
                        }
                        Err(err) => Poll::Ready(Some(Err(anyhow::Error::msg(err)))),
                    },
                    None => Poll::Ready(None),
                }
            }
            z => Poll::Ready(z),
        }
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use futures::StreamExt;
    use object_store::{memory::InMemory, ObjectStore};

    use crate::{
        model::schema::{AllType, PrimitiveType, SchemaV2, Struct, StructField},
        table::table_builder::TableBuilder,
    };

    #[tokio::test]
    async fn test_files_stream() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let schema = SchemaV2 {
            schema_id: 1,
            identifier_field_ids: Some(vec![1, 2]),
            name_mapping: None,
            struct_fields: Struct {
                fields: vec![
                    StructField {
                        id: 1,
                        name: "one".to_string(),
                        required: false,
                        field_type: AllType::Primitive(PrimitiveType::String),
                        doc: None,
                    },
                    StructField {
                        id: 2,
                        name: "two".to_string(),
                        required: false,
                        field_type: AllType::Primitive(PrimitiveType::String),
                        doc: None,
                    },
                ],
            },
        };
        let mut table =
            TableBuilder::new_filesystem_table("test/append", schema, Arc::clone(&object_store))
                .unwrap()
                .commit()
                .await
                .unwrap();

        table
            .new_transaction()
            .fast_append(vec![
                "test/append/data/file1.parquet".to_string(),
                "test/append/data/file2.parquet".to_string(),
            ])
            .commit()
            .await
            .unwrap();
        table
            .new_transaction()
            .fast_append(vec![
                "test/append/data/file3.parquet".to_string(),
                "test/append/data/file4.parquet".to_string(),
            ])
            .commit()
            .await
            .unwrap();
        let mut files = table
            .files()
            .await
            .unwrap()
            .map(|manifest_entry| manifest_entry.map(|x| x.data_file.file_path));
        assert_eq!(
            files.next().await.unwrap().unwrap(),
            "test/append/data/file1.parquet".to_string()
        );
        assert_eq!(
            files.next().await.unwrap().unwrap(),
            "test/append/data/file2.parquet".to_string()
        );
        assert_eq!(
            files.next().await.unwrap().unwrap(),
            "test/append/data/file3.parquet".to_string()
        );
        assert_eq!(
            files.next().await.unwrap().unwrap(),
            "test/append/data/file4.parquet".to_string()
        );
    }
}
