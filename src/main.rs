use std::{
    io::Write,
    path::{Path, PathBuf},
    time::Duration,
};

use input::JsonLinesRecv;
use output::OutputThreadPool;

mod data;
mod input;
mod output;
mod thread_pool;

#[derive(Debug, Clone)]
pub enum ReadError {
    EndOfInputReached,
    InvalidLine(String),
}

#[derive(Debug, Clone)]
pub enum ErrorKind {
    ReadErr(ReadError),
}

#[derive(Debug, Clone)]
pub struct Error {
    kind: Box<ErrorKind>,
}

impl From<ReadError> for Error {
    fn from(value: ReadError) -> Self {
        Self {
            kind: Box::new(ErrorKind::ReadErr(value)),
        }
    }
}

pub struct RunCfg {
    input_file: PathBuf,
    output_dir: PathBuf,
    output_threads: usize,
}

pub fn run(cfg: RunCfg) {
    let input = std::fs::File::open(cfg.input_file).unwrap();
    let lines = JsonLinesRecv::spawn_new(input);

    let mut output = OutputThreadPool::new(cfg.output_threads, cfg.output_dir);

    for line in lines {
        let line = match line {
            Ok(l) => l,
            Err(ReadError::EndOfInputReached) => {
                unreachable!()
            }
            Err(e) => panic!("Error encountered: {e:?}"),
        };

        output.write_line(line);
    }
}

fn run_input1() {
    run(RunCfg {
        input_file: "./example_sets/input1.json.gz".try_into().unwrap(),
        output_dir: "./example_sets/out/".try_into().unwrap(),
        output_threads: 10,
    })
}

fn run_testlines() {
    let input = std::fs::File::open::<PathBuf>("./example_sets/input1.json.gz".try_into().unwrap())
        .unwrap();
    let lines = JsonLinesRecv::spawn_new(input);
    for line in lines {
        match line {
            Ok(ln) => println!("{}:\n==> {ln}", ln.key().path_to(Path::new("~")).display()),
            Err(e) => println!("ERR: {e:?}"),
        }
    }
}

fn main() {
    run_input1();
}
