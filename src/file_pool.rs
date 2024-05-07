use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::PathBuf,
};

use tokio_uring::fs::{File, OpenOptions};

use crate::data::{MsgKey, MsgKeyMap, MsgKeySet};

/// A `FilePool` file that is open.
/// Any `FilePoolEntry` items should be returned to their `FilePool` instead of being dropped
pub struct FilePoolEntry {
    pub cursor: usize,
    pub file: File,
}

impl FilePoolEntry {
    pub async fn write_all(&mut self, mut to_write: Vec<u8>) -> Result<(), std::io::Error> {
        loop {
            if to_write.len() == 0 {
                break;
            }
            let (written, mut same_buf) = self.file.write_at(to_write, self.cursor as u64).await;
            let written = written?;

            self.cursor += written;
            to_write = same_buf.split_off(written);
        }
        Ok(())
    }
}

struct FilePoolEntryInactive {
    cursor: usize,
    closing_task: tokio::task::JoinHandle<std::io::Result<()>>,
}

/// Represents a pool of files with a limit on how many can be open at once
///
/// The only way to obtain is a file is to [`take()`](FilePool::take),
/// which will return a [`FilePoolEntry`](FilePoolEntry)
pub struct FilePool {
    max_open_files: usize,
    root: PathBuf,
    /// This is a FIFO queue representing how recently a given file has been used.
    /// The elements in this queue are the same as the keys in `idle_files`
    ///
    /// When a file is given back to this file pool, it will be pushed to the back of this queue
    ///
    /// When a file must be temporarily closed to stay under the `max_open_files`,
    /// the idle file at the front of this queue will be chosen
    idle_files_queue: VecDeque<MsgKey>,
    idle_files: MsgKeyMap<FilePoolEntry>,
    taken_files: MsgKeySet,
    inactive_files: MsgKeyMap<FilePoolEntryInactive>,
}

impl FilePool {
    /// Creates a file pool which will not open more than the specified number of files at once
    pub fn new(max_open_files: usize, root: PathBuf) -> Self {
        Self {
            max_open_files,
            root,
            idle_files_queue: Default::default(),
            idle_files: Default::default(),
            taken_files: Default::default(),
            inactive_files: Default::default(),
        }
    }

    /// Returns `true` iff no file handles are being kept by this pool
    ///
    /// Before dropping this pool, this should return `true`
    pub fn has_no_file_handles(&self) -> bool {
        self.idle_files.len() == 0
    }

    fn open_files(&self) -> usize {
        self.idle_files.len() + self.taken_files.len()
    }

    /// Pops the top of `self.idle_files_queue`, and flushes that file.
    /// Then, moves that file to `self.inactive_files`
    ///
    /// Panics:
    /// * If there is no file which can be closed
    async fn close_file(&mut self) {
        let to_close_key = self
            .idle_files_queue
            .pop_front()
            .expect("There was no file to close! (idle_files was empty)");
        let FilePoolEntry {
            cursor,
            file: to_close,
        } = self.idle_files.remove(&to_close_key).expect("unreachable!");
        let h = tokio_uring::spawn(async move {
            // NOTE: dropping a `tokio_uring` file does not ensure all data is written to disk!
            to_close.sync_all().await?;
            to_close.close().await?;
            Ok(())
        });

        let inactive = FilePoolEntryInactive {
            cursor,
            closing_task: h,
        };
        assert!(self.inactive_files.insert(to_close_key, inactive).is_none())
    }

    /// Tries to take the given file, creating a new file if it didn't exist.
    /// The returned `FilePoolEntry` **must** be given back (using [`give`](FilePool::give)).
    ///
    /// Dropping files must be done via the [`finish`](FilePool::finish) method
    ///
    /// Panics:
    /// * If the file is already taken
    /// * If taking this file would mean exceeding the `max_open_files` specified when creating this file pool
    pub async fn take(&mut self, to_take: MsgKey) -> FilePoolEntry {
        assert!(
            !self.taken_files.contains(&to_take),
            "Tried to take a file that was already taken!"
        );
        assert!(
            self.taken_files.len() < self.max_open_files,
            "Tried to take file when the maximum number of files has already been taken ({})!",
            self.max_open_files
        );

        if self.idle_files.contains_key(&to_take) {
            // This file is already open, just idle (not taken)

            let queue_pos = self
                .idle_files_queue
                .iter()
                .position(|k| k == &to_take)
                .expect("unreachable!");
            self.idle_files_queue.remove(queue_pos).unwrap();

            let f = self.idle_files.remove(&to_take).expect("unreachable!");
            assert!(self.taken_files.insert(to_take));

            f
        } else if self.inactive_files.contains_key(&to_take) {
            // This file needs to be re-opened

            if self.open_files() >= self.max_open_files {
                self.close_file().await;
            }

            let FilePoolEntryInactive {
                cursor,
                closing_task,
            } = self.inactive_files.remove(&to_take).unwrap();

            // Make sure the file gets properly flushed before re-opening
            closing_task.await.unwrap().unwrap();

            let path = to_take.path_to(&self.root);
            let file = OpenOptions::new().write(true).open(path).await.unwrap();
            let entry = FilePoolEntry { cursor, file };
            assert!(self.taken_files.insert(to_take));
            entry
        } else {
            // A new file must be created

            if self.open_files() >= self.max_open_files {
                self.close_file().await;
            }

            let path = to_take.path_to(&self.root);
            let file = File::create(path).await.unwrap();
            let entry = FilePoolEntry { cursor: 0, file };
            assert!(self.taken_files.insert(to_take));
            entry
        }
    }
    /// Gives this `FilePool` back ownership over a file.
    ///
    /// Panics if `entry` is not currently taken
    #[track_caller]
    pub fn give(&mut self, key: MsgKey, entry: FilePoolEntry) {
        assert!(
            self.taken_files.remove(&key),
            "Tried to give file that was not taken!"
        );

        // NOTE: this operation will not change `self.open_files()`, since we are removing from `taken` and adding to `idle`
        assert!(self.idle_files.insert(key.clone(), entry).is_none());
        self.idle_files_queue.push_back(key);
    }
    pub async fn finish(&mut self) {
        for _i in 0..self.idle_files.len() {
            self.close_file().await;
        }
        for (_, entry) in self.inactive_files.drain() {
            entry.closing_task.await.unwrap().unwrap();
        }
        assert!(self.idle_files.is_empty());
    }
}
