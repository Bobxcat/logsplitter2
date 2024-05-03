use std::{
    collections::HashMap,
    io::{stdout, Write},
    path::PathBuf,
    time::{Duration, Instant},
};

use flate2::{write::GzEncoder, Compression};
use kanal::{Receiver, Sender};
use tokio_uring::fs::File;

use crate::{
    data::{LineData, MsgKey},
    thread_pool::ThreadPool,
};

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
                    println!("Closed one down!");
                    tx.close();
                    break;
                } else if start.elapsed() > TIMEOUT {
                    // panic!("Timeout elapsed when trying to call `OutputThreadPool::finish`. Timeout was {TIMEOUT:?}")
                }
            }
        }
        self.pool.join();
    }
}

impl Drop for OutputThreadPool {
    fn drop(&mut self) {
        self.finish()
    }
}

fn output_task(rx: Receiver<LineData>, out: std::fs::File) {
    tokio_uring::start(async move {
        let rx_lines = rx.as_async();

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
                        println!("CLOSED");
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
