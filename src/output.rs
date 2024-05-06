use std::{
    collections::{HashMap, VecDeque},
    io::Write,
    path::PathBuf,
    sync::{Arc, Mutex},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use flate2::{write::GzEncoder, Compression};
use kanal::{Receiver, Sender};
use tokio_uring::fs::File;

use crate::{
    data::{LineData, MsgKey},
    file_pool::FilePool,
    math_utils,
    thread_pool::ThreadPool,
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
    msgkey_assigned: HashMap<MsgKey, usize>,
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
                let (tx, rx) = kanal::bounded(100);
                let h = std::thread::spawn(move || {
                    let files = FilePool::new(max_files, root_dir);
                    tokio_uring::start(async move { output_thread(rx, files).await })
                });
                ThreadInfo { h, tx }
            })
            .collect();

        Self {
            threads,
            msgkey_assigned: HashMap::new(),
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

    pub fn finish(&mut self) {
        println!("Started finishing output files...");

        let threads = self.threads.drain(..).collect::<Vec<_>>();

        threads
            .iter()
            .for_each(|t| t.tx.send(OutputThreadMsg::Finish).unwrap());

        threads.iter().for_each(|t| {
            let start = Instant::now();
            loop {
                if t.tx.is_empty() {
                    t.tx.close();
                    break;
                }
                const TIMEOUT: Duration = Duration::from_millis(10_000);
                if start.elapsed() > TIMEOUT {
                    panic!("Timeout elapsed when trying to `finish` a thread! {TIMEOUT:?}")
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
    let mut encoders: HashMap<MsgKey, GzEncoder<Vec<u8>>> = HashMap::new();

    loop {
        match rx.recv().await.expect(
            "Main thread closed unexpectedly! /
            `Finish` should have been sent",
        ) {
            OutputThreadMsg::Finish => {
                for (key, enc) in encoders {
                    let to_write = enc.finish().unwrap();

                    let mut f = files.take(key).await;
                    f.write_all(to_write).await.unwrap();
                }

                assert!(files.has_no_idle_or_inactive_files());
                rx.close();
                return;
            }
            OutputThreadMsg::Write { ln } => {
                let key = ln.key().clone();
                let mut f = files.take(key.clone()).await;
                let enc = encoders
                    .entry(key.clone())
                    .or_insert_with(|| GzEncoder::new(vec![], Compression::default()));
                enc.write_all(ln.original_line_text().as_bytes()).unwrap();
                if enc.get_ref().len() >= 1024 {
                    let to_write = std::mem::replace(enc.get_mut(), vec![]);
                    f.write_all(to_write).await.unwrap();
                }
                files.give(key, f);
            }
        }
    }
}

pub struct OutputThreadPool {
    root_dir: PathBuf,
    pool: ThreadPool,
    active_files: HashMap<MsgKey, Sender<LineData>>,
}

impl OutputThreadPool {
    pub fn new(_num_threads: usize, root_dir: PathBuf) -> Self {
        Self {
            // pool: ThreadPoolBuilder::new()
            //     .num_threads(num_threads)
            //     .build()
            //     .unwrap(),
            pool: ThreadPool::new(),
            root_dir,
            active_files: HashMap::new(),
        }
    }
    pub fn write_line(&mut self, ln: LineData) {
        let tx = self
            .active_files
            .entry(ln.key().clone())
            .or_insert_with(|| {
                let path = ln.key().path_to(&self.root_dir);
                println!("Creating {}", path.display());
                let (tx, rx) = kanal::unbounded();
                let output_file = std::fs::File::create(path).unwrap();
                self.pool.spawn(move || {
                    output_task(rx, output_file);
                });
                tx
            });

        tx.send(ln).unwrap();
    }
    pub fn finish(&mut self) {
        println!("Started closing down threads!");
        println!("  Currently active files: {}", self.active_files.len());
        for (_, tx) in &mut self.active_files {
            tx.send(LineData::end_of_input()).unwrap();
        }
        for (_, tx) in &mut self.active_files {
            let start = Instant::now();
            loop {
                const TIMEOUT: Duration = Duration::from_millis(10_000);
                if tx.is_empty() {
                    tx.close();
                    break;
                } else if start.elapsed() > TIMEOUT {
                    panic!("Timeout elapsed when trying to call `OutputThreadPool::finish`. Timeout was {TIMEOUT:?}")
                }
            }
        }
        self.pool.join();
        println!("Closed down all active threads successfully!")
    }
}

impl Drop for OutputThreadPool {
    fn drop(&mut self) {
        self.finish()
    }
}

fn output_task(rx_lines: Receiver<LineData>, out: std::fs::File) {
    tokio_uring::start(async move {
        let rx_lines = rx_lines.as_async();

        // Send encoded data to be written to this ouput file
        // IMPORTANT: Empty vector signals shutdown!
        let (tx_encoded, rx_encoded) = kanal::bounded_async::<Vec<u8>>(10);

        tokio_uring::spawn(async move {
            let mut buf = vec![];
            let out = File::from_std(out);
            let mut cursor = 0;
            loop {
                let next_data = rx_encoded.recv().await.unwrap();
                let eoi_detected = next_data.len() == 0;
                buf.extend(next_data.into_iter());

                if eoi_detected {
                    loop {
                        let (res, mut same_buf) = out.write_at(buf, cursor).await;
                        let written = res.unwrap();

                        if written == 0 {
                            out.close().await.unwrap();
                            rx_encoded.close();
                            return;
                        } else {
                            buf = same_buf.split_off(written);
                            cursor += written as u64;
                        }
                    }
                }

                // TODO: Maybe use `writev_at` to avoid as much moving?
                let (written, mut same_buf) = out.write_at(buf, cursor).await;
                let written = written.unwrap();
                buf = same_buf.split_off(written);
                cursor += written as u64;
            }
        });

        let mut enc = GzEncoder::new(vec![], Compression::default());

        loop {
            let ln = rx_lines.recv().await.expect("Main thread closed unexpectedly! `OutputTaskPool::finish()` should have been called.");

            if ln == LineData::end_of_input() {
                // let enc_finish = enc.finish().unwrap();
                let enc_finish: Vec<u8> = loop {
                    match enc.try_finish() {
                        Ok(_) => break std::mem::replace(enc.get_mut(), vec![]),
                        Err(_) => (),
                    }
                };

                if enc_finish.len() > 0 {
                    tx_encoded.send(enc_finish).await.unwrap();
                }
                tx_encoded.send(vec![]).await.unwrap();

                loop {
                    if tx_encoded.is_closed() {
                        return;
                    }
                    // For some reason, `tokio::task::yield_now()` didn't work.
                    // This call is needed so that the file writing loop has a chance to do its thing
                    tokio::time::sleep(Duration::from_nanos(1)).await;
                }
            } else {
                enc.write_all(ln.original_line_text().as_bytes()).unwrap();

                let enc_buf = std::mem::replace(enc.get_mut(), vec![]);
                if enc_buf.len() > 0 {
                    tx_encoded.send(enc_buf).await.unwrap();
                }
            }
        }
    })
}
