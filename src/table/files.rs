/*!
 * Helper for iterating over files in a table.
*/
use std::{
    fs::File,
    iter::Map,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use anyhow::{anyhow, Result};
use apache_avro::{types::Value, Reader};
use futures::Stream;
use object_store::{path::Path, GetResult, ObjectStore};

use crate::model::{manifest::ManifestEntry, manifest_list::ManifestFile};

use super::Table;

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
        Reader<'list, File>,
        fn(Result<Value, apache_avro::Error>) -> Result<ManifestFile, apache_avro::Error>,
    >,
    manifest_iter: Option<
        Map<
            Reader<'manifest, File>,
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
                            let mut result = object_store.get(&path);
                            match Pin::as_mut(&mut result).poll(cx) {
                                Poll::Ready(file) => {
                                    let bytes = if let GetResult::File(file, _) =
                                        file.map_err(anyhow::Error::msg)?
                                    {
                                        Ok(file)
                                    } else {
                                        Err(anyhow!(""))
                                    }?;
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
                            }
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

impl Table {
    /// Get files associated to a table
    pub async fn files<'list, 'manifest>(
        &'list self,
    ) -> Result<impl Stream<Item = Result<ManifestEntry>>> {
        let snapshot = if let Some(snapshots) = &self.metadata().snapshots {
            Ok(&snapshots[snapshots.len()])
        } else {
            Err(anyhow!("No snapshots in this table."))
        }?;
        let object_store = self.object_store();
        let path: Path = snapshot.manifest_list.clone().into();
        let bytes = if let GetResult::File(file, _) =
            object_store.get(&path).await.map_err(anyhow::Error::msg)?
        {
            Ok(file)
        } else {
            Err(anyhow!(""))
        }?;
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
