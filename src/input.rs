use std::{
    collections::VecDeque,
    io::{Read, Write},
};

use flate2::write::MultiGzDecoder;
use kanal::{ReceiveError, Receiver, Sender};
use tokio_uring::fs::File;

use crate::{byte_channel, data::LineData, ReadError};

pub struct JsonLinesRecv {
    rx_raw: Receiver<String>,
}

impl JsonLinesRecv {
    pub fn spawn_new(input: std::fs::File) -> Self {
        let (tx, rx) = kanal::bounded::<String>(100);

        std::thread::spawn(move || {
            tokio_uring::start(async {
                let input = File::from_std(input);
                read_input(input, tx).await
            })
        });

        Self { rx_raw: rx }
    }
}

impl Iterator for JsonLinesRecv {
    type Item = Result<LineData, ReadError>;

    fn next(&mut self) -> Option<Self::Item> {
        let ln = match self.rx_raw.recv() {
            Ok(s) => s,
            Err(ReceiveError::Closed) | Err(ReceiveError::SendClosed) => return None,
        };
        let data = LineData::parse(&ln);

        match data {
            Ok(s) => Some(Ok(s)),
            Err(ReadError::EndOfInputReached) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

struct FileRead {
    f: File,
    cursor: u64,
}

impl FileRead {
    pub async fn read_next(&mut self) -> std::io::Result<Vec<u8>> {
        let v = vec![0; 1024];
        let (written, mut v) = self.f.read_at(v, self.cursor).await;
        let written = written?;
        v.truncate(written);
        self.cursor += written as u64;
        Ok(v)
    }
}

async fn read_input(input: File, tx: Sender<String>) {
    let mut input = FileRead {
        f: input,
        cursor: 0,
    };
    let (tx_decoded, mut rx_decoded) = byte_channel::bounded(100);

    let mut dec = MultiGzDecoder::new(tx_decoded);
    let mut curr_line = String::new();

    loop {
        let to_decode = input.read_next().await.unwrap();
        dec.write_all(&to_decode).unwrap();

        if to_decode.len() == 0 {
            dec.flush().unwrap();

            // Duplicated
            while let Some(b) = rx_decoded.try_recv() {
                if b == b'\n' {
                    tx.send(curr_line).unwrap();
                    curr_line = String::new();
                } else {
                    // Don't include newlines in the json
                    curr_line.push(b as char);
                }
            }

            if curr_line.len() > 0 {
                tx.send(curr_line).unwrap();
            }
            loop {
                if tx.is_empty() {
                    tx.close();
                    return;
                }
            }
        }

        // Duplicated
        while let Some(b) = rx_decoded.try_recv() {
            if b == b'\n' {
                tx.send(curr_line).unwrap();
                curr_line = String::new();
            } else {
                // Don't include newlines in the json
                curr_line.push(b as char);
            }
        }
    }
}

/// Sends every line of the input `.json.gz` file until all lines have been read from `tx`, then `tx` is closed
async fn reading_input(input: File, tx: Sender<String>) {
    todo!()
    // // Sender for the raw file data
    // // IMPORTANT: Empty vector indicates EOI
    // let (tx_encoded, rx_encoded) = kanal::bounded_async::<Vec<u8>>(100);

    // let _h = tokio_uring::spawn(async move {
    //     let mut cursor = 0;
    //     loop {
    //         let buf = vec![0; 1024];
    //         let (bytes_read, mut buf) = input.read_at(buf, cursor).await;
    //         let bytes_read = bytes_read.unwrap();
    //         buf.truncate(bytes_read);

    //         cursor += bytes_read as u64;
    //         tx_encoded.send(buf).await.unwrap();
    //     }
    // });

    // let mut dec = MultiGzDecoder::new(VecDeque::new());
    // let mut curr_line = String::new();

    // loop {
    //     let to_decode = rx_encoded.recv().await.unwrap();
    //     let eoi_reached = to_decode.len() == 0;
    //     dec.write_all(&to_decode).unwrap();

    //     if eoi_reached {
    //         // let decoded = dec.finish().unwrap();
    //         dec.flush().unwrap();
    //         let mut decoded = vec![];
    //         let written = dec.read_to_end(&mut decoded).unwrap();
    //         decoded.truncate(written);

    //         println!("DECODED (FINAL): {written}");

    //         for b in decoded {
    //             if b == b'\n' {
    //                 tx.send(curr_line).unwrap();
    //                 curr_line = String::new();
    //             } else {
    //                 // Don't include newlines in the json
    //                 curr_line.push(b as char);
    //             }
    //         }

    //         if curr_line.len() > 0 {
    //             tx.send(curr_line).unwrap();
    //         }
    //         loop {
    //             if tx.is_empty() {
    //                 tx.close();
    //                 return;
    //             }
    //         }
    //     }

    //     // We can only take all of the bytes at once because we assume ASCII
    //     let mut decoded = vec![0; 1024];
    //     let written = match dec.read(&mut decoded) {
    //         Ok(w) => w,
    //         Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => continue,
    //         Err(e) => panic!("{e}"),
    //     };
    //     decoded.truncate(written);

    //     if written > 0 {
    //         println!("DECODED: {written}");
    //     }

    //     for b in decoded {
    //         if b == b'\n' {
    //             tx.send(curr_line).unwrap();
    //             curr_line = String::new();
    //         } else {
    //             // Don't include newlines in the json
    //             curr_line.push(b as char);
    //         }
    //     }
    // }
}
