use std::{
    path::{Path as FsPath, PathBuf},
    sync::Arc,
};

use futures::StreamExt;
use object_store::{
    ObjectMeta, ObjectStore,
    local::LocalFileSystem,
    path::{Error as ObjectPathError, Path as StorePath},
};
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::{StorageBackend, StorageConfig};

pub type ObjectPath = StorePath;

#[derive(Clone)]
pub struct Storage {
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
}

impl Storage {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: ObjectPath) -> Self {
        Self { store, prefix }
    }

    pub fn from_config(config: &StorageConfig) -> Result<Self, StorageError> {
        match config.backend {
            StorageBackend::Local => {
                let root = config.resolved_local_path();
                let prefix = prefix_from_config(config.prefix.as_deref())?;
                let store = LocalFileSystem::new_with_prefix(&root).map_err(|source| {
                    StorageError::CreateLocalBackend {
                        root: root.clone(),
                        source,
                    }
                })?;

                Ok(Self::new(Arc::new(store), prefix))
            }
            _ => Err(StorageError::UnsupportedBackend {
                backend: config.backend.clone(),
            }),
        }
    }

    pub fn new_local(
        root: impl AsRef<FsPath>,
        prefix: Option<impl AsRef<str>>,
    ) -> Result<Self, StorageError> {
        let root = root.as_ref().to_path_buf();
        let prefix = prefix_from_config(prefix.as_ref().map(AsRef::as_ref))?;
        let store = LocalFileSystem::new_with_prefix(&root).map_err(|source| {
            StorageError::CreateLocalBackend {
                root: root.clone(),
                source,
            }
        })?;

        Ok(Self::new(Arc::new(store), prefix))
    }

    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.store)
    }

    pub fn prefix(&self) -> &ObjectPath {
        &self.prefix
    }

    pub async fn put_json<T>(&self, path: &ObjectPath, value: &T) -> Result<(), StorageError>
    where
        T: Serialize + ?Sized,
    {
        let bytes = serde_json::to_vec(value).map_err(|source| StorageError::SerializeJson {
            path: path.clone(),
            source,
        })?;
        self.put_bytes(path, bytes).await
    }

    pub async fn get_json<T>(&self, path: &ObjectPath) -> Result<T, StorageError>
    where
        T: DeserializeOwned,
    {
        let bytes = self.get_bytes(path).await?;
        serde_json::from_slice(&bytes).map_err(|source| StorageError::DeserializeJson {
            path: path.clone(),
            source,
        })
    }

    pub async fn put_bytes(
        &self,
        path: &ObjectPath,
        bytes: impl Into<Vec<u8>>,
    ) -> Result<(), StorageError> {
        let full_path = self.qualify_path(path);
        self.store
            .put(&full_path, bytes.into().into())
            .await
            .map_err(|source| StorageError::PutObject {
                path: path.clone(),
                source,
            })?;
        Ok(())
    }

    pub async fn get_bytes(&self, path: &ObjectPath) -> Result<Vec<u8>, StorageError> {
        let full_path = self.qualify_path(path);
        let bytes = self
            .store
            .get(&full_path)
            .await
            .map_err(|source| StorageError::GetObject {
                path: path.clone(),
                source,
            })?
            .bytes()
            .await
            .map_err(|source| StorageError::GetObject {
                path: path.clone(),
                source,
            })?;
        Ok(bytes.to_vec())
    }

    pub async fn delete(&self, path: &ObjectPath) -> Result<(), StorageError> {
        let full_path = self.qualify_path(path);
        self.store
            .delete(&full_path)
            .await
            .map_err(|source| StorageError::DeleteObject {
                path: path.clone(),
                source,
            })
    }

    pub async fn list_prefix(&self, prefix: &ObjectPath) -> Result<Vec<ObjectMeta>, StorageError> {
        let full_prefix = self.qualify_path(prefix);
        let mut stream = self.store.list(Some(&full_prefix));
        let mut entries = Vec::new();

        while let Some(item) = stream.next().await {
            let meta = item.map_err(|source| StorageError::ListPrefix {
                prefix: prefix.clone(),
                source,
            })?;
            entries.push(ObjectMeta {
                location: self.strip_prefix(&meta.location)?,
                ..meta
            });
        }

        Ok(entries)
    }

    pub async fn exists(&self, path: &ObjectPath) -> Result<bool, StorageError> {
        let full_path = self.qualify_path(path);
        match self.store.head(&full_path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(source) => Err(StorageError::HeadObject {
                path: path.clone(),
                source,
            }),
        }
    }

    fn qualify_path(&self, path: &ObjectPath) -> ObjectPath {
        if self.prefix.as_ref().is_empty() {
            path.clone()
        } else if path.as_ref().is_empty() {
            self.prefix.clone()
        } else {
            self.prefix.parts().chain(path.parts()).collect()
        }
    }

    fn strip_prefix(&self, path: &ObjectPath) -> Result<ObjectPath, StorageError> {
        if self.prefix.as_ref().is_empty() {
            return Ok(path.clone());
        }

        let suffix =
            path.prefix_match(&self.prefix)
                .ok_or_else(|| StorageError::PrefixMismatch {
                    path: path.clone(),
                    prefix: self.prefix.clone(),
                })?;

        Ok(suffix.collect())
    }
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("storage backend '{backend}' is not supported; only 'local' is currently available")]
    UnsupportedBackend { backend: StorageBackend },
    #[error("failed to create local object store at '{}': {source}", root.display())]
    CreateLocalBackend {
        root: PathBuf,
        #[source]
        source: object_store::Error,
    },
    #[error("invalid storage prefix '{prefix}': {source}")]
    InvalidPrefix {
        prefix: String,
        #[source]
        source: ObjectPathError,
    },
    #[error("failed to write object '{path}': {source}")]
    PutObject {
        path: ObjectPath,
        #[source]
        source: object_store::Error,
    },
    #[error("failed to read object '{path}': {source}")]
    GetObject {
        path: ObjectPath,
        #[source]
        source: object_store::Error,
    },
    #[error("failed to delete object '{path}': {source}")]
    DeleteObject {
        path: ObjectPath,
        #[source]
        source: object_store::Error,
    },
    #[error("failed to list prefix '{prefix}': {source}")]
    ListPrefix {
        prefix: ObjectPath,
        #[source]
        source: object_store::Error,
    },
    #[error("failed to check object '{path}': {source}")]
    HeadObject {
        path: ObjectPath,
        #[source]
        source: object_store::Error,
    },
    #[error("failed to serialize JSON for '{path}': {source}")]
    SerializeJson {
        path: ObjectPath,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to deserialize JSON for '{path}': {source}")]
    DeserializeJson {
        path: ObjectPath,
        #[source]
        source: serde_json::Error,
    },
    #[error("object store returned path '{path}' outside configured prefix '{prefix}'")]
    PrefixMismatch {
        path: ObjectPath,
        prefix: ObjectPath,
    },
}

fn prefix_from_config(prefix: Option<&str>) -> Result<ObjectPath, StorageError> {
    match prefix {
        Some(prefix) => ObjectPath::parse(prefix).map_err(|source| StorageError::InvalidPrefix {
            prefix: prefix.to_owned(),
            source,
        }),
        None => Ok(ObjectPath::default()),
    }
}

pub mod paths {
    use super::ObjectPath;
    use thiserror::Error;

    #[derive(Debug, Error)]
    pub enum PathComponentError {
        #[error("invalid escaped path component '{value}': incomplete percent escape")]
        IncompleteEscape { value: String },
        #[error("invalid escaped path component '{value}': bad hex digits '{digits}'")]
        InvalidHex { value: String, digits: String },
        #[error("invalid escaped path component '{value}': {source}")]
        InvalidUtf8 {
            value: String,
            #[source]
            source: std::string::FromUtf8Error,
        },
    }

    pub fn unescape_component(value: &str) -> Result<String, PathComponentError> {
        let bytes = value.as_bytes();
        let mut decoded = Vec::with_capacity(bytes.len());
        let mut index = 0;

        while index < bytes.len() {
            if bytes[index] == b'%' {
                if index + 2 >= bytes.len() {
                    return Err(PathComponentError::IncompleteEscape {
                        value: value.to_owned(),
                    });
                }

                let digits = &value[index + 1..index + 3];
                let parsed =
                    u8::from_str_radix(digits, 16).map_err(|_| PathComponentError::InvalidHex {
                        value: value.to_owned(),
                        digits: digits.to_owned(),
                    })?;
                decoded.push(parsed);
                index += 3;
            } else {
                decoded.push(bytes[index]);
                index += 1;
            }
        }

        String::from_utf8(decoded).map_err(|source| PathComponentError::InvalidUtf8 {
            value: value.to_owned(),
            source,
        })
    }

    pub fn escape_component(value: &str) -> String {
        ObjectPath::from_iter([value]).to_string()
    }

    pub fn session_root(session_id: &str) -> ObjectPath {
        ObjectPath::from("sessions").child(format!("session_id={session_id}"))
    }

    pub fn session_manifest(session_id: &str) -> ObjectPath {
        session_root(session_id).child("manifest.json")
    }

    pub fn session_wal_root(session_id: &str) -> ObjectPath {
        ObjectPath::from("wal").child(format!("session_id={session_id}"))
    }

    pub fn wal_stream_root(session_id: &str, node_id: &str, output_id: &str) -> ObjectPath {
        session_wal_root(session_id)
            .child(format!("node_id={node_id}"))
            .child(format!("output_id={output_id}"))
    }

    pub fn wal_segment(
        session_id: &str,
        node_id: &str,
        output_id: &str,
        segment_file_name: &str,
    ) -> ObjectPath {
        wal_stream_root(session_id, node_id, output_id).child(segment_file_name)
    }

    pub fn parquet_root(node_id: &str, output_id: &str, schema_fingerprint: &str) -> ObjectPath {
        global_parquet_root()
            .child(format!("node_id={node_id}"))
            .child(format!("output_id={output_id}"))
            .child(format!("schema_fingerprint={schema_fingerprint}"))
    }

    pub fn global_parquet_root() -> ObjectPath {
        ObjectPath::from("parquet")
    }

    pub fn parquet_file(
        node_id: &str,
        output_id: &str,
        schema_fingerprint: &str,
        file_name: &str,
    ) -> ObjectPath {
        parquet_root(node_id, output_id, schema_fingerprint).child(file_name)
    }

    pub fn catalog_events_prefix() -> ObjectPath {
        ObjectPath::from_iter(["catalog", "events"])
    }

    pub fn catalog_event(file_name: &str) -> ObjectPath {
        catalog_events_prefix().child(file_name)
    }

    pub fn schema_catalog_dir() -> ObjectPath {
        ObjectPath::from_iter(["catalog", "schemas"])
    }

    pub fn schema_file(schema_fingerprint: &str) -> ObjectPath {
        schema_catalog_dir().child(format!("{schema_fingerprint}.arrow.json"))
    }
}

#[cfg(test)]
mod tests {
    use super::{paths, *};
    use tempfile::TempDir;

    #[derive(Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
    struct ExampleJson {
        name: String,
        count: u64,
    }

    #[tokio::test]
    async fn local_storage_reads_and_writes_json_and_bytes() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let storage = Storage::new_local(temp_dir.path(), None::<&str>).expect("storage");
        let json_path = ObjectPath::from("catalog/example.json");
        let bytes_path = ObjectPath::from("catalog/example.bin");

        storage
            .put_json(
                &json_path,
                &ExampleJson {
                    name: "demo".to_owned(),
                    count: 3,
                },
            )
            .await
            .expect("json should be written");
        storage
            .put_bytes(&bytes_path, b"amber".to_vec())
            .await
            .expect("bytes should be written");

        assert_eq!(
            storage
                .get_json::<ExampleJson>(&json_path)
                .await
                .expect("json should load"),
            ExampleJson {
                name: "demo".to_owned(),
                count: 3,
            }
        );
        assert_eq!(
            storage
                .get_bytes(&bytes_path)
                .await
                .expect("bytes should load"),
            b"amber".to_vec()
        );
        assert!(
            storage
                .exists(&json_path)
                .await
                .expect("exists should work")
        );
    }

    #[tokio::test]
    async fn list_and_delete_respect_configured_prefix() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let storage = Storage::new_local(temp_dir.path(), Some("tenant/dev")).expect("storage");
        let first = ObjectPath::from("catalog/events/0001.json");
        let second = ObjectPath::from("catalog/events/0002.json");

        storage
            .put_bytes(&first, b"one".to_vec())
            .await
            .expect("first object should be written");
        storage
            .put_bytes(&second, b"two".to_vec())
            .await
            .expect("second object should be written");

        let entries = storage
            .list_prefix(&ObjectPath::from("catalog/events"))
            .await
            .expect("list should work");
        let mut listed_paths = entries
            .into_iter()
            .map(|meta| meta.location.to_string())
            .collect::<Vec<_>>();
        listed_paths.sort();

        assert_eq!(
            listed_paths,
            vec![
                "catalog/events/0001.json".to_owned(),
                "catalog/events/0002.json".to_owned()
            ]
        );

        storage.delete(&first).await.expect("delete should work");

        assert!(!storage.exists(&first).await.expect("exists should work"));
        assert!(storage.exists(&second).await.expect("exists should work"));
    }

    #[test]
    fn from_config_uses_local_root_and_prefix() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let config = StorageConfig {
            backend: StorageBackend::Local,
            path: Some(temp_dir.path().to_path_buf()),
            bucket: None,
            prefix: Some("robots/dev".to_owned()),
            endpoint: None,
            access_key: None,
            secret_key: None,
        };

        let storage = Storage::from_config(&config).expect("storage should build");

        assert_eq!(storage.prefix().as_ref(), "robots/dev");
    }

    #[test]
    fn path_helpers_are_data_dir_relative_and_escape_segments() {
        assert_eq!(paths::escape_component("joint/states"), "joint%2Fstates");
        assert_eq!(
            paths::unescape_component("joint%2Fstates").expect("component should decode"),
            "joint/states"
        );
        assert_eq!(
            paths::session_manifest("20260509_abc123").as_ref(),
            "sessions/session_id=20260509_abc123/manifest.json"
        );
        assert_eq!(
            paths::session_wal_root("20260509_abc123").as_ref(),
            "wal/session_id=20260509_abc123"
        );
        assert_eq!(
            paths::wal_segment(
                "20260509_abc123",
                "joint/states",
                "state/raw",
                "segment-0001.arrow"
            )
            .as_ref(),
            "wal/session_id=20260509_abc123/node_id=joint%2Fstates/output_id=state%2Fraw/segment-0001.arrow"
        );
        assert_eq!(paths::global_parquet_root().as_ref(), "parquet");
        assert_eq!(
            paths::parquet_file("joint/states", "state/raw", "abc123", "part-0001.parquet")
                .as_ref(),
            "parquet/node_id=joint%2Fstates/output_id=state%2Fraw/schema_fingerprint=abc123/part-0001.parquet"
        );
        assert_eq!(
            paths::catalog_event("0001-compaction_committed.json").as_ref(),
            "catalog/events/0001-compaction_committed.json"
        );
        assert_eq!(
            paths::schema_file("abc123").as_ref(),
            "catalog/schemas/abc123.arrow.json"
        );
        assert_eq!(paths::schema_catalog_dir().as_ref(), "catalog/schemas");
    }

    #[test]
    fn unescape_component_rejects_invalid_percent_encoding() {
        let error =
            paths::unescape_component("joint%2").expect_err("truncated percent escape should fail");
        assert!(matches!(
            error,
            paths::PathComponentError::IncompleteEscape { .. }
        ));
    }
}
