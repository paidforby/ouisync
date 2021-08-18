use crate::{
    blob::Blob,
    branch::Branch,
    directory::{Directory, EntryData},
    entry_type::EntryType,
    error::Result,
    locator::Locator,
    path,
    version_vector::VersionVector,
};
use camino::{Utf8Component, Utf8PathBuf};
use std::ops::DerefMut;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Context needed for updating all necessary info when writing to a file or directory.
pub struct WriteContext {
    // None iff this WriteContext corresponds to the root directory.
    parent: Option<Parent>,
    inner: Mutex<Inner>,
}

#[derive(Clone)]
struct Parent {
    directory: Directory,
    entry_name: String,
    entry_data: Arc<EntryData>,
    // TODO: Should this be std::sync::Weak?
}

struct Inner {
    ancestors: Vec<Directory>,
}

impl WriteContext {
    pub fn root() -> Self {
        Self {
            parent: None,
            inner: Mutex::new(Inner {
                ancestors: Vec::new(),
            }),
        }
    }

    pub async fn child(
        &self,
        parent_directory: Directory,
        entry_name: String,
        entry_data: Arc<EntryData>,
    ) -> Self {
        Self {
            parent: Some(Parent {
                directory: parent_directory,
                entry_name,
                entry_data,
            }),
            inner: Mutex::new(Inner {
                ancestors: Vec::new(),
            }),
        }
    }

    /// Begin writing to the given blob. This ensures the blob lives in the local branch and all
    /// its ancestor directories exist and live in the local branch as well.
    /// Call `commit` to finalize the write.
    pub async fn begin(
        &self,
        local_branch: &Branch,
        entry_type: EntryType,
        blob: &mut Blob,
    ) -> Result<()> {
        // let mut ancestors = vec![];
        // let mut current = self.parent.cloned();

        // while let Some(current) = current {
        //     let next = current.directory.write_context
        //     ancestors.push(current);
        //     current = current.read().await.parent().clone();
        // }

        // todo!()

        // TODO: load the directories always

        let mut guard = self.inner.lock().await;
        let inner = guard.deref_mut();

        if blob.branch().id() == local_branch.id() {
            // Blob already lives in the local branch. We assume the ancestor directories have been
            // already created as well so there is nothing else to do.
            return Ok(());
        }

        let dst_locator =
            if let Some((parent, name)) = path::decompose(&self.calculate_path().await) {
                inner.ancestors = local_branch.ensure_directory_exists(parent).await?;
                let vv = self.version_vector().clone();
                inner
                    .ancestors
                    .last_mut()
                    .unwrap()
                    .insert_entry(name.to_owned(), entry_type, vv)
                    .await?
                    .locator()
            } else {
                // `blob` is the root directory.
                Locator::Root
            };

        blob.fork(local_branch.clone(), dst_locator).await
    }

    /// Commit writing to the blob started by a previous call to `begin`. Does nothing if `begin`
    /// was not called.
    pub async fn commit(&self) -> Result<()> {
        let mut guard = self.inner.lock().await;
        let inner = guard.deref_mut();

        let mut dirs = inner.ancestors.drain(..).rev();

        for component in self.calculate_path().await.components().rev() {
            match component {
                Utf8Component::Normal(name) => {
                    if let Some(dir) = dirs.next() {
                        dir.increment_entry_version(name).await?;
                        dir.apply().await?;
                    } else {
                        break;
                    }
                }
                Utf8Component::Prefix(_) | Utf8Component::RootDir | Utf8Component::CurDir => (),
                Utf8Component::ParentDir => panic!("non-normalized paths not supported"),
            }
        }

        Ok(())
    }

    async fn calculate_path(&self) -> Utf8PathBuf {
        let mut next = self.parent.clone();
        let mut path = Utf8PathBuf::from("/");

        while let Some(current) = next {
            path = path.join(&current.entry_name);
            next = current
                .directory
                .read()
                .await
                .write_context()
                .parent
                .clone();
        }

        path
    }

    fn version_vector(&self) -> &VersionVector {
        // TODO: How do we get the VV when this WriteContext corresponds to the root directory?
        self.parent.as_ref().unwrap().entry_data.version_vector()
    }
}
