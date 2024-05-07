use std::io::Write;

use kanal::{Receiver, Sender};

/// The supplied bound is how many chunks of bytes can be buffered at once
pub fn bounded(bound: usize) -> (BytesTx, BytesRx) {
    let (tx, rx) = kanal::bounded(bound);
    (
        BytesTx { tx },
        BytesRx {
            rx,
            buffered: vec![],
            buffered_idx: 0,
        },
    )
}

pub struct BytesTx {
    tx: Sender<Vec<u8>>,
}

impl Write for BytesTx {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.tx
            .send(buf.to_vec())
            .expect("Receiver closed earlier than expected!");
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub struct BytesRx {
    rx: Receiver<Vec<u8>>,
    buffered: Vec<u8>,
    buffered_idx: usize,
}

impl BytesRx {
    #[track_caller]
    pub fn try_recv(&mut self) -> Option<u8> {
        if self.buffered_idx >= self.buffered.len() {
            match self.rx.try_recv() {
                Ok(Some(new_buf)) => {
                    self.buffered_idx = 0;
                    self.buffered = new_buf;
                }
                Ok(None) => return None,
                Err(_) => panic!("Closed unexpectedly!"),
            }
        }

        let b = self.buffered[self.buffered_idx];
        self.buffered_idx += 1;
        Some(b)
    }
}
