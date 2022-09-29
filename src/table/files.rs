/*!
 * Helper for iterating over files in a table.
*/
use std::{
    io::Cursor,
    iter::{repeat, FilterMap, Map, Zip},
    pin::Pin,
    slice::Iter,
    sync::Arc,
    task::{Context, Poll},
};

use anyhow::Result;
use apache_avro::{types::Value as AvroValue, Reader};
use futures::{Future, Stream, TryFutureExt};
use object_store::{path::Path, ObjectStore};

use crate::model::{manifest::ManifestEntry, manifest_list::ManifestFile};

use super::Table;

impl Table {
    /// Get a stream of files associated to a table. The files are returned based on the list of manifest files associated to the table.
    /// The included manifest files can be filtered based on an filter vector. The filter vector has the length equal to the number of manifest files
    /// and contains a true entry everywhere the manifest file is to be included in the output.
    pub async fn files<'file>(
        &self,
        filter: Option<Vec<bool>>,
    ) -> Result<impl Stream<Item = Result<ManifestEntry>> + '_> {
        let manifests = match filter {
            Some(predicate) => {
                self.manifests()
                    .iter()
                    .zip(Box::new(predicate.into_iter())
                        as Box<dyn Iterator<Item = bool> + Send + Sync>)
                    .filter_map(
                        filter_manifest
                            as fn((&'file ManifestFile, bool)) -> Option<&'file ManifestFile>,
                    )
            }
            None => self
                .manifests()
                .iter()
                .zip(Box::new(repeat(true)) as Box<dyn Iterator<Item = bool> + Send + Sync>)
                .filter_map(
                    filter_manifest
                        as fn((&'file ManifestFile, bool)) -> Option<&'file ManifestFile>,
                ),
        };
        Ok(DataFileStream {
            object_store: self.object_store(),
            manifest_list_iter: manifests,
            manifest_iter: None,
        })
    }
}

fn filter_manifest((manifest, predicate): (&ManifestFile, bool)) -> Option<&ManifestFile> {
    if predicate {
        Some(manifest)
    } else {
        None
    }
}

fn avro_value_to_manifest_entry(
    entry: Result<AvroValue, apache_avro::Error>,
) -> Result<ManifestEntry, anyhow::Error> {
    entry
        .and_then(|value| apache_avro::from_value(&value))
        .map_err(anyhow::Error::msg)
}

type ManifestListIter<'list> = FilterMap<
    Zip<Iter<'list, ManifestFile>, Box<dyn Iterator<Item = bool> + Send + Sync>>,
    fn((&'list ManifestFile, bool)) -> Option<&'list ManifestFile>,
>;

type ManifestIter<'manifest> = Option<
    Map<
        Reader<'manifest, Cursor<Vec<u8>>>,
        fn(Result<AvroValue, apache_avro::Error>) -> Result<ManifestEntry, anyhow::Error>,
    >,
>;

/// Iterator over all files in a given snapshot
pub struct DataFileStream<'list, 'manifest> {
    object_store: Arc<dyn ObjectStore>,
    manifest_list_iter: ManifestListIter<'list>,
    manifest_iter: ManifestIter<'manifest>,
}

impl<'list, 'manifest> Stream for DataFileStream<'list, 'manifest> {
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
                    Some(file) => {
                        let object_store = Arc::clone(&self.object_store);
                        let path: Path = file.manifest_path().into();
                        let result = object_store.get(&path).and_then(|file| file.bytes());
                        let temp = match Pin::as_mut(&mut Box::pin(result)).poll(cx) {
                            Poll::Ready(file) => {
                                let bytes = Cursor::new(Vec::from(file?));
                                let mut reader = apache_avro::Reader::new(bytes)?;
                                let next = reader.next();
                                self.manifest_iter = Some(reader.map(
                                    avro_value_to_manifest_entry
                                        as fn(
                                            Result<AvroValue, apache_avro::Error>,
                                        )
                                            -> Result<ManifestEntry, anyhow::Error>,
                                ));
                                Poll::Ready(next.map(avro_value_to_manifest_entry))
                            }
                            Poll::Pending => Poll::Pending,
                        };
                        temp
                    }
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
        model::schema::{AllType, PrimitiveType, SchemaStruct, SchemaV2, StructField},
        table::table_builder::TableBuilder,
    };

    #[tokio::test]
    async fn test_files_stream() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let schema = SchemaV2 {
            schema_id: 1,
            identifier_field_ids: Some(vec![1, 2]),
            name_mapping: None,
            struct_fields: SchemaStruct {
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
            .files(None)
            .await
            .unwrap()
            .map(|manifest_entry| manifest_entry.map(|x| x.file_path().to_string()));
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
