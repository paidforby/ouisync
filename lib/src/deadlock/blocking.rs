use super::timer::{Id, Timer};
use core::ops::{Deref, DerefMut};
use once_cell::sync::Lazy;
use std::{
    backtrace::Backtrace,
    panic::Location,
    sync, thread,
    time::{Duration, Instant},
};

static TIMER: Timer<WatchedEntry> = Timer::new();
static WATCHING_THREAD: Lazy<thread::JoinHandle<()>> = Lazy::new(|| thread::spawn(watching_thread));

const WARNING_TIMEOUT: Duration = Duration::from_secs(5);

/// A Mutex that reports to the standard output when it's not released within WARNING_TIMEOUT
/// duration.
pub struct Mutex<T: ?Sized> {
    inner: sync::Mutex<T>,
}

impl<T> Mutex<T> {
    pub fn new(t: T) -> Self {
        Self {
            inner: sync::Mutex::new(t),
        }
    }
}

impl<T: ?Sized> Mutex<T> {
    // NOTE: using `track_caller` so that the `Location` constructed inside points to where
    // this function is called and not inside it.
    #[track_caller]
    pub fn lock(&self) -> sync::LockResult<MutexGuard<'_, T>> {
        // Make sure the thread is instantiated. Is it better to do this here or in the
        // `Mutex::new` function?
        let _ = *WATCHING_THREAD;

        let entry = WatchedEntry {
            file_and_line: Location::caller(),
            backtrace: Backtrace::capture(),
        };
        let deadline = Instant::now() + WARNING_TIMEOUT;
        let entry_id = TIMER.schedule(deadline, entry);

        let lock_result = self
            .inner
            .lock()
            .map(|inner| MutexGuard { entry_id, inner })
            .map_err(|err| {
                sync::PoisonError::new(MutexGuard {
                    entry_id,
                    inner: err.into_inner(),
                })
            });

        if lock_result.is_err() {
            // MutexGuard was not created, so we need to remove it ourselves.
            TIMER.cancel(entry_id);
        }

        lock_result
    }
}

pub struct MutexGuard<'a, T: ?Sized + 'a> {
    entry_id: Id,
    inner: sync::MutexGuard<'a, T>,
}

impl<'a, T: ?Sized> Drop for MutexGuard<'a, T> {
    fn drop(&mut self) {
        if TIMER.cancel(self.entry_id).is_none() {
            // Using `println!` and not `tracing::*` to avoid circular dependencies because on
            // Android tracing uses `StateMonitor` which uses these mutexes.
            println!(
                "Previously reported blocking mutex (id:{}) got released.",
                self.entry_id
            );
        }
    }
}

impl<'a, T: ?Sized> Deref for MutexGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.inner.deref()
    }
}

impl<'a, T: ?Sized> DerefMut for MutexGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.inner.deref_mut()
    }
}

struct WatchedEntry {
    file_and_line: &'static Location<'static>,
    backtrace: Backtrace,
}

fn watching_thread() {
    loop {
        let (entry_id, entry) = TIMER.wait();

        // Using `println!` and not `tracing::*` to avoid circular dependencies because on
        // Android tracing uses `StateMonitor` which uses these mutexes.
        println!(
            "Possible blocking deadlock (id:{}) at:\n{}\n{}\n",
            entry_id, entry.file_and_line, entry.backtrace
        );
    }
}
