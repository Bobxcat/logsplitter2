use std::{
    collections::{HashMap, VecDeque},
    io::{Read, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use flate2::{write::GzEncoder, Compression};
use kanal::{Receiver, Sender};

use crate::{
    byte_channel::{self, BytesRx, BytesTx},
    data::{LineData, MsgKey, MsgKeyMap},
    file_pool::FilePool,
    math_utils,
};

/// Sent from main thread to output writing thread
enum OutputThreadMsg {
    Finish,
    Write { ln: LineData },
}

struct ThreadInfo {
    h: JoinHandle<()>,
    tx: Sender<OutputThreadMsg>,
}

/// A list of output files. Output files are never removed until [`finish`](OutputFiles::finish) is called
///
/// Each thread keeps track of their own list of active and inactive files
pub struct OutputFiles {
    threads: Vec<ThreadInfo>,
    /// The thread which each `MsgKey` will be routed to
    msgkey_assigned: MsgKeyMap<usize>,
    /// The thread which most recently had a new `MsgKey` assigned to it
    last_thread_with_new_file: usize,
}

impl OutputFiles {
    pub fn new(num_threads: usize, max_active_files: usize, root_dir: PathBuf) -> Self {
        assert!(
            max_active_files >= num_threads,
            "Cannot have `max_active_threads` < `num_threads`"
        );

        let threads = math_utils::get_even_partition(num_threads, max_active_files)
            .into_iter()
            .map(|max_files| {
                let root_dir = root_dir.clone();
                let (tx, rx) = kanal::bounded(256);
                let h = std::thread::spawn(move || {
                    let files = FilePool::new(max_files, root_dir);
                    tokio_uring::start(async move { output_thread(rx, files).await })
                });
                ThreadInfo { h, tx }
            })
            .collect();

        Self {
            threads,
            msgkey_assigned: Default::default(),
            last_thread_with_new_file: 0,
        }
    }

    pub fn write_line(&mut self, ln: LineData) {
        let thread_idx = *self
            .msgkey_assigned
            .entry(ln.key().clone())
            .or_insert_with(|| {
                let t = self.last_thread_with_new_file;
                self.last_thread_with_new_file = (t + 1) % self.threads.len();
                t
            });

        self.threads[thread_idx]
            .tx
            .send(OutputThreadMsg::Write { ln })
            .unwrap();
    }

    fn finish(&mut self) {
        println!("Started finishing output files...");

        let threads = self.threads.drain(..).collect::<Vec<_>>();

        threads
            .iter()
            .for_each(|t| t.tx.send(OutputThreadMsg::Finish).unwrap());

        println!("Waiting for thread channels to flush...");

        threads.iter().for_each(|t| {
            let start = Instant::now();
            loop {
                if t.tx.is_empty() {
                    t.tx.close();
                    break;
                }
                const TIMEOUT: Duration = Duration::from_millis(10_000);
                if start.elapsed() > TIMEOUT {
                    // panic!("Timeout elapsed when trying to `finish` a thread! {TIMEOUT:?}")
                }
            }
        });

        println!("Joining threads...");
        threads.into_iter().for_each(|t| {
            t.h.join().unwrap();
        });
        println!("Output files finished successfully!")
    }
}

impl Drop for OutputFiles {
    fn drop(&mut self) {
        self.finish();
    }
}

/// The `files` parameter here should be empty
async fn output_thread(rx: Receiver<OutputThreadMsg>, mut files: FilePool) {
    let rx = rx.as_async();
    let mut encoders: HashMap<MsgKey, (flate2::write::GzEncoder<BytesTx>, BytesRx)> =
        HashMap::new();

    loop {
        match rx.recv().await.expect(
            "Main thread closed unexpectedly! /
            `Finish` should have been sent",
        ) {
            OutputThreadMsg::Finish => {
                for (key, mut enc) in encoders {
                    enc.0.flush().unwrap();

                    let mut to_write = vec![];
                    while let Some(b) = enc.1.try_recv() {
                        to_write.push(b);
                    }

                    let mut f = files.take(key.clone()).await;
                    f.write_all(to_write).await.unwrap();
                    files.give(key, f);
                }

                files.finish().await;

                assert!(files.has_no_file_handles());
                rx.close();
                return;
            }
            OutputThreadMsg::Write { ln } => {
                let key = ln.key().clone();
                let mut f = files.take(key.clone()).await;
                let enc = encoders.entry(key.clone()).or_insert_with(|| {
                    let (tx, rx) = byte_channel::bounded(16);
                    (GzEncoder::new(tx, Compression::default()), rx)
                });
                enc.0.write_all(ln.original_line_text().as_bytes()).unwrap();

                let mut to_write = vec![];
                while let Some(b) = enc.1.try_recv() {
                    to_write.push(b);
                }
                if to_write.len() > 0 {
                    f.write_all(to_write).await.unwrap();
                }

                files.give(key, f);
            }
        }
    }
}

/// The `files` parameter here should be empty
async fn output_thread_old(rx: Receiver<OutputThreadMsg>, mut files: FilePool) {
    let rx = rx.as_async();
    let mut encoders: HashMap<MsgKey, GzEncoder<VecDeque<u8>>> = HashMap::new();

    loop {
        match rx.recv().await.expect(
            "Main thread closed unexpectedly! /
            `Finish` should have been sent",
        ) {
            OutputThreadMsg::Finish => {
                for (key, mut enc) in encoders {
                    enc.flush().unwrap();

                    let mut to_write = vec![];
                    let written = enc.read_to_end(&mut to_write).unwrap();
                    to_write.truncate(written);

                    let mut f = files.take(key.clone()).await;
                    f.write_all(to_write).await.unwrap();
                    files.give(key, f);
                }

                files.finish().await;

                assert!(files.has_no_file_handles());
                rx.close();
                return;
            }
            OutputThreadMsg::Write { ln } => {
                let key = ln.key().clone();
                let mut f = files.take(key.clone()).await;
                let enc = encoders
                    .entry(key.clone())
                    .or_insert_with(|| GzEncoder::new(Default::default(), Compression::default()));
                enc.write_all(ln.original_line_text().as_bytes()).unwrap();

                let mut to_write = vec![0; 1024];
                let written = enc.read(&mut to_write).unwrap();
                to_write.truncate(written);
                if to_write.len() > 0 {
                    f.write_all(to_write).await.unwrap();
                }

                files.give(key, f);
            }
        }
    }
}
