use std::{collections::VecDeque, io::Write};

use flate2::write::MultiGzDecoder;
use kanal::{ReceiveError, Receiver, Sender};
use tokio_uring::fs::File;

use crate::{data::LineData, ReadError};

pub struct JsonLinesRecv {
    rx_raw: Receiver<String>,
}

impl JsonLinesRecv {
    pub fn spawn_new(input: std::fs::File) -> Self {
        let (tx, rx) = kanal::unbounded::<String>();

        std::thread::spawn(move || {
            tokio_uring::start(async {
                let input = File::from_std(input);
                reading_input(input, tx).await
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

/// Sends every line of the input `.json.gz` file until all lines have been read from `tx`, then `tx` is closed
async fn reading_input(input: File, tx: Sender<String>) {
    // Sender for the raw file data
    // IMPORTANT: Empty vector indicates EOI
    let (tx_encoded, rx_encoded) = kanal::unbounded_async::<Vec<u8>>();

    let _h = tokio_uring::spawn(async move {
        let mut cursor = 0;
        loop {
            let buf = vec![0; 1024];
            let (bytes_read, mut buf) = input.read_at(buf, cursor).await;
            let bytes_read = bytes_read.unwrap();
            let _ = buf.split_off(bytes_read);

            cursor += bytes_read as u64;
            tx_encoded.send(buf).await.unwrap();
        }
    });

    let mut dec = MultiGzDecoder::new(Vec::new());
    let mut curr_line = String::new();

    loop {
        let to_decode = rx_encoded.recv().await.unwrap();
        let eoi_reached = to_decode.len() == 0;
        dec.write_all(&to_decode).unwrap();

        if eoi_reached {
            let decoded = dec.finish().unwrap();
            for b in decoded {
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

        // We can only take all of the bytes at once because we assume ASCII
        let decoded = std::mem::replace(dec.get_mut(), vec![]);

        for b in decoded {
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
