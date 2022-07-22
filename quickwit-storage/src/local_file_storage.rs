// Copyright (C) 2022 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::io::{ErrorKind, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fmt, io};

use async_trait::async_trait;
use futures::future::{BoxFuture, FutureExt};
use quickwit_common::uri::{Protocol, Uri};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tracing::warn;

use crate::{
    DebouncedStorage, OwnedBytes, Storage, StorageError, StorageErrorKind, StorageFactory,
    StorageResolverError, StorageResult,
};

/// File system compatible storage implementation.
#[derive(Clone)]
pub struct LocalFileStorage {
    uri: Uri,
    root: PathBuf,
}

impl fmt::Debug for LocalFileStorage {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("LocalFileStorage")
            .field("root", &self.root.display())
            .finish()
    }
}

impl LocalFileStorage {
    /// Creates a local file storage instance given a URI.
    pub fn from_uri(uri: &Uri) -> Result<Self, StorageResolverError> {
        uri.filepath()
            .map(|root| Self {
                uri: uri.clone(),
                root: root.to_path_buf(),
            })
            .ok_or_else(|| StorageResolverError::InvalidUri {
                message: format!("URI `{uri}` is not a valid file URI."),
            })
    }

    /// Moves a file from a source to a destination.
    /// from here is an external path, and to is an internal path.
    pub async fn move_into(&self, from_external: &Path, to: &Path) -> crate::StorageResult<()> {
        let to_full_path = self.root.join(to);
        fs::rename(from_external, to_full_path).await?;
        Ok(())
    }

    /// Moves a file from a source to a destination.
    /// from here is an internal path, and to is an external path.
    pub async fn move_out(&self, from_internal: &Path, to: &Path) -> crate::StorageResult<()> {
        let from_full_path = self.root.join(from_internal);
        fs::rename(from_full_path, to).await?;
        Ok(())
    }
}

/// Delete empty directories starting from `{root}/{path}` directory and stopping at `{root}`
/// directory. Note that the `{root}` directory is not deleted.
fn delete_all_dirs(root: PathBuf, path: &Path) -> BoxFuture<'_, std::io::Result<()>> {
    async move {
        let full_path = root.join(path);
        let path_entries_result = full_path.read_dir();
        if let Err(err) = &path_entries_result {
            // Ignore `ErrorKind::NotFound` as this could be deleted by another concurent task.
            if err.kind() == ErrorKind::NotFound {
                return Ok(());
            }
        }

        let is_not_empty = path_entries_result?.next().is_some();
        if is_not_empty {
            return Ok(());
        }

        let delete_result = fs::remove_dir(full_path).await;
        if let Err(err) = &delete_result {
            // Ignore `ErrorKind::NotFound` as this could be deleted by another concurent task.
            if err.kind() == ErrorKind::NotFound {
                return Ok(());
            }
            delete_result?;
        }

        match &path.parent() {
            Some(path) => {
                if path == &Path::new("") || path == &Path::new(".") {
                    return Ok(());
                }
                delete_all_dirs(root, path).await?;
            }
            _ => return Ok(()),
        }

        Ok(())
    }
    .boxed()
}

fn missing_file_is_ok(io_result: io::Result<()>) -> io::Result<()> {
    match io_result {
        Ok(()) => Ok(()),
        Err(io_err) if io_err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(io_err) => Err(io_err),
    }
}

#[async_trait]
impl Storage for LocalFileStorage {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        if !self.root.exists() {
            // By creating directories, we check if we have the right permissions.
            fs::create_dir_all(self.root.as_path()).await?
        }
        Ok(())
    }

    async fn put(
        &self,
        path: &Path,
        payload: Box<dyn crate::PutPayload>,
    ) -> crate::StorageResult<()> {
        let full_path = self.root.join(path);
        if let Some(parent_dir) = full_path.parent() {
            fs::create_dir_all(parent_dir).await?;
        }

        let mut reader = payload.byte_stream().await?.into_async_read();
        let mut f = tokio::fs::File::create(full_path).await?;
        tokio::io::copy(&mut reader, &mut f).await?;

        Ok(())
    }

    async fn copy_to_file(&self, path: &Path, output_path: &Path) -> StorageResult<()> {
        let full_path = self.root.join(path);
        fs::copy(full_path, output_path).await?;
        Ok(())
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        let full_path = self.root.join(path);
        let mut file = fs::File::open(full_path).await?;
        file.seek(SeekFrom::Start(range.start as u64)).await?;
        let mut content_bytes: Vec<u8> = vec![0u8; range.len()];
        file.read_exact(&mut content_bytes).await?;
        Ok(OwnedBytes::new(content_bytes))
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        let full_path = self.root.join(path);
        missing_file_is_ok(fs::remove_file(full_path).await)?;
        let parent = path.parent();
        if parent.is_none() {
            return Ok(());
        }
        if let Err(error) = delete_all_dirs(self.root.to_path_buf(), parent.unwrap()).await {
            warn!(error = ?error, path = %path.display(), "Failed to clean up parent directories.");
        }
        Ok(())
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        let full_path = self.root.join(path);
        let content_bytes = fs::read(full_path).await.map_err(|err| {
            StorageError::from(err).add_context(format!(
                "Failed to read file {}/{}",
                self.uri(),
                path.to_string_lossy()
            ))
        })?;
        Ok(OwnedBytes::new(content_bytes))
    }

    fn uri(&self) -> &Uri {
        &self.uri
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        let full_path = self.root.join(path);
        match fs::metadata(full_path).await {
            Ok(metadata) => {
                if metadata.is_file() {
                    Ok(metadata.len())
                } else {
                    Err(StorageErrorKind::DoesNotExist.with_error(anyhow::anyhow!(
                        "File `{}` is actually a directory.",
                        path.display()
                    )))
                }
            }
            Err(err) => {
                if err.kind() == ErrorKind::NotFound {
                    Err(StorageErrorKind::DoesNotExist.with_error(err))
                } else {
                    Err(err.into())
                }
            }
        }
    }
}

/// A File storage resolver
#[derive(Debug, Clone, Default)]
pub struct LocalFileStorageFactory {}

impl StorageFactory for LocalFileStorageFactory {
    fn protocol(&self) -> Protocol {
        Protocol::File
    }

    fn resolve(&self, uri: &Uri) -> Result<Arc<dyn Storage>, StorageResolverError> {
        let storage = LocalFileStorage::from_uri(uri)?;
        Ok(Arc::new(DebouncedStorage::new(storage)))
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::test_suite::storage_test_suite;

    #[tokio::test]
    async fn test_storage() -> anyhow::Result<()> {
        let uri = Uri::try_new(&format!("{}", tempdir()?.path().display())).unwrap();
        let mut file_storage = LocalFileStorage::from_uri(&uri)?;
        storage_test_suite(&mut file_storage).await?;
        Ok(())
    }

    #[test]
    fn test_file_storage_factory() -> anyhow::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let index_uri = Uri::new(format!("file://{}/foo/bar", tempdir.path().display()));
        let file_storage_factory = LocalFileStorageFactory::default();
        let storage = file_storage_factory.resolve(&index_uri)?;
        assert_eq!(storage.uri(), &index_uri);

        let err = file_storage_factory
            .resolve(&Uri::new("s3://foo/bar".to_string()))
            .err()
            .unwrap();
        assert!(matches!(err, StorageResolverError::InvalidUri { .. }));

        let err = file_storage_factory
            .resolve(&Uri::new("s3://".to_string()))
            .err()
            .unwrap();
        assert!(matches!(err, StorageResolverError::InvalidUri { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn test_try_delete_dir_all() -> anyhow::Result<()> {
        let path_root = tempdir()?.into_path();
        let dir_path = path_root.clone().join("foo/bar/baz");
        tokio::fs::create_dir_all(dir_path.clone()).await?;

        // check all empty directory
        assert_eq!(dir_path.exists(), true);
        delete_all_dirs(path_root.clone(), dir_path.as_path()).await?;
        assert_eq!(dir_path.exists(), false);
        assert_eq!(dir_path.parent().unwrap().exists(), false);

        // check with intermediate file
        tokio::fs::create_dir_all(dir_path.clone()).await?;
        let intermediate_file = dir_path.parent().unwrap().join("fizz.txt");
        tokio::fs::File::create(intermediate_file.clone()).await?;
        assert_eq!(dir_path.exists(), true);
        assert_eq!(intermediate_file.exists(), true);
        delete_all_dirs(path_root.clone(), dir_path.as_path()).await?;
        assert_eq!(dir_path.exists(), false);
        assert_eq!(dir_path.parent().unwrap().exists(), true);

        // make sure it does not go beyond the path
        tokio::fs::create_dir_all(path_root.join("home/foo/bar")).await?;
        delete_all_dirs(path_root.join("home/foo"), Path::new("bar")).await?;
        assert_eq!(path_root.join("home/foo").exists(), true);

        Ok(())
    }
}
