use crate::{registry::Handle, repository::RepositoryHolder, state::State};
use camino::Utf8PathBuf;
use deadlock::AsyncMutex;
use ouisync_lib::{Branch, File};
use std::io::SeekFrom;

pub struct FileHolder {
    pub(crate) file: AsyncMutex<File>,
    pub(crate) local_branch: Option<Branch>,
}

pub(crate) async fn open(
    state: &State,
    repo: Handle<RepositoryHolder>,
    path: Utf8PathBuf,
) -> Result<Handle<FileHolder>, ouisync_lib::Error> {
    let repo = state.get_repository(repo);
    let local_branch = repo.repository.local_branch().ok();

    let file = repo.repository.open_file(&path).await?;
    let holder = FileHolder {
        file: AsyncMutex::new(file),
        local_branch,
    };
    let handle = state.files.insert(holder);

    Ok(handle)
}

pub(crate) async fn create(
    state: &State,
    repo: Handle<RepositoryHolder>,
    path: Utf8PathBuf,
) -> Result<Handle<FileHolder>, ouisync_lib::Error> {
    let repo = state.get_repository(repo);
    let local_branch = repo.repository.local_branch()?;

    let file = repo.repository.create_file(&path).await?;
    let holder = FileHolder {
        file: AsyncMutex::new(file),
        local_branch: Some(local_branch),
    };
    let handle = state.files.insert(holder);

    Ok(handle)
}

/// Remove (delete) the file at the given path from the repository.
pub(crate) async fn remove(
    state: &State,
    repo: Handle<RepositoryHolder>,
    path: Utf8PathBuf,
) -> Result<(), ouisync_lib::Error> {
    state
        .get_repository(repo)
        .repository
        .remove_entry(&path)
        .await?;
    Ok(())
}

pub(crate) async fn close(
    state: &State,
    handle: Handle<FileHolder>,
) -> Result<(), ouisync_lib::Error> {
    if let Some(holder) = state.files.remove(handle) {
        holder.file.lock().await.flush().await?
    }

    Ok(())
}

pub(crate) async fn flush(
    state: &State,
    handle: Handle<FileHolder>,
) -> Result<(), ouisync_lib::Error> {
    state.files.get(handle).file.lock().await.flush().await?;
    Ok(())
}

/// Read at most `len` bytes from the file and returns them. The returned buffer can be shorter
/// than `len` and empty in case of EOF.
pub(crate) async fn read(
    state: &State,
    handle: Handle<FileHolder>,
    offset: u64,
    len: u64,
) -> Result<Vec<u8>, ouisync_lib::Error> {
    let len = len as usize;
    let mut buffer = vec![0; len];

    let holder = state.files.get(handle);
    let mut file = holder.file.lock().await;

    file.seek(SeekFrom::Start(offset));

    // TODO: consider using just `read`
    let len = file.read_all(&mut buffer).await?;
    buffer.truncate(len);

    Ok(buffer)
}

/// Write `len` bytes from `buffer` into the file.
pub(crate) async fn write(
    state: &State,
    handle: Handle<FileHolder>,
    offset: u64,
    buffer: Vec<u8>,
) -> Result<(), ouisync_lib::Error> {
    let holder = state.files.get(handle);
    let mut file = holder.file.lock().await;

    let local_branch = holder
        .local_branch
        .as_ref()
        .ok_or(ouisync_lib::Error::PermissionDenied)?
        .clone();

    file.seek(SeekFrom::Start(offset));
    file.fork(local_branch).await?;

    // TODO: consider using just `write` and returning the number of bytes written
    file.write_all(&buffer).await?;

    Ok(())
}

/// Truncate the file to `len` bytes.
pub(crate) async fn truncate(
    state: &State,
    handle: Handle<FileHolder>,
    len: u64,
) -> Result<(), ouisync_lib::Error> {
    let holder = state.files.get(handle);

    let mut file = holder.file.lock().await;

    let local_branch = holder
        .local_branch
        .as_ref()
        .ok_or(ouisync_lib::Error::PermissionDenied)?
        .clone();

    file.fork(local_branch).await?;
    file.truncate(len)?;

    Ok(())
}

/// Retrieve the total size of the file in bytes.
pub(crate) async fn len(state: &State, handle: Handle<FileHolder>) -> u64 {
    state.files.get(handle).file.lock().await.len()
}

/// Retrieve the sync progress of the file.
pub(crate) async fn progress(
    state: &State,
    handle: Handle<FileHolder>,
) -> Result<u64, ouisync_lib::Error> {
    // Don't keep the file locked while progress is being awaited.
    let progress = state.files.get(handle).file.lock().await.progress();
    let progress = progress.await?;

    Ok(progress)
}
