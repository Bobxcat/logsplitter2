use std::{
    fs::File,
    path::{Path, PathBuf},
};

use input::JsonLinesRecv;
use output::OutputFiles;
use tempdir::TempDir;
use testdata_gen::{generate_testdata, TestdataCfg};

mod data;
mod file_pool;
mod input;
mod math_utils;
mod output;
mod testdata_gen;

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
    #[allow(unused)]
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
    std::fs::create_dir_all(&cfg.output_dir).unwrap();

    let input = std::fs::File::open(cfg.input_file).unwrap();
    let lines = JsonLinesRecv::spawn_new(input);

    let mut output = OutputFiles::new(cfg.output_threads, 64, cfg.output_dir);
    // let mut output = OutputThreadPool::new(cfg.output_threads, cfg.output_dir);

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
        output_threads: 8,
    })
}

fn run_ryan1() {
    run(RunCfg {
        input_file: "./example_sets/ryan1.json.gz".try_into().unwrap(),
        output_dir: "./example_sets/out_ryan1/".try_into().unwrap(),
        output_threads: 8,
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

fn run_generated(cfg: TestdataCfg) {
    let path_input = Path::new("./example_sets/rand/input.json.gz");
    let path_input_dbg = Path::new("./example_sets/rand/input.json");
    let path_output = Path::new("./example_sets/rand/out/");

    std::fs::create_dir_all(path_output).unwrap();

    generate_testdata(
        cfg,
        &mut File::create(path_input).unwrap(),
        &mut File::create(path_input_dbg).unwrap(),
    )
    .unwrap();

    println!("Testdata generated!");

    run(RunCfg {
        input_file: path_input.into(),
        output_dir: path_output.into(),
        output_threads: 1,
    })
}

fn main() {
    // run_input1();
    // run_ryan1();
    run_generated(TestdataCfg {
        lines: 10_000,
        ..Default::default()
    })
}
