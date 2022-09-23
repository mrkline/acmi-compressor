use std::{
    fs::File,
    io::{prelude::*, BufReader, BufWriter},
};

use anyhow::{bail, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use clap::Parser;
use crossbeam::channel::{bounded, Receiver};
use log::*;
use simplelog::*;
use tacview::{record::Record, ParseError};

#[derive(Debug, Parser)]
struct Args {
    /// Verbosity (-v, -vv, -vvv, etc.)
    #[clap(short, long, parse(from_occurrences))]
    verbose: u8,

    #[clap(short, long, arg_enum, default_value = "auto")]
    color: Color,

    acmi: Utf8PathBuf,
}

#[derive(Debug, Copy, Clone, clap::ArgEnum)]
enum Color {
    Auto,
    Always,
    Never,
}

#[allow(clippy::large_enum_variant)]
enum Reader<'a> {
    Uncompressed(tacview::Parser<BufReader<&'a mut File>>),
    Compressed(tacview::Parser<zip::read::ZipFile<'a>>),
}

impl Iterator for Reader<'_> {
    type Item = Result<Record, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Uncompressed(u) => u.next(),
            Self::Compressed(c) => c.next(),
        }
    }
}

impl<'a> Reader<'a> {
    fn new(name: &Utf8Path, fh: &'a mut File) -> Result<Self> {
        let r = if name.as_str().ends_with(".zip.acmi") {
            // No need for BufReader, DEFLATE (ZIP compression) has its own buffer.
            Reader::Compressed(tacview::Parser::new_compressed(fh)?)
        } else {
            Reader::Uncompressed(tacview::Parser::new(BufReader::new(fh))?)
        };
        Ok(r)
    }
}

fn run() -> Result<()> {
    let args = Args::parse();
    init_logger(&args);

    if args.acmi == "-" {
        bail!("Reading from stdin currently unsupported (can't seek that!)");
    }

    let mut fh = File::open(&args.acmi).context("Couldn't open ACMI")?;

    let (tx, rx) = bounded(1024);

    std::thread::scope(|s| {
        let write_thread = s.spawn(move || writer_thread(rx));

        let read_thread = s.spawn(move || {
            let reader = Reader::new(&args.acmi, &mut fh)?;
            for rec in reader {
                if tx.send(rec?).is_err() {
                    break;
                }
            }
            anyhow::Ok(())
        });

        write_thread.join().expect("Couldn't join writer thread")?;
        read_thread.join().expect("Couldn't join reader thread")?;
        anyhow::Ok(())
    })?;

    Ok(())
}

fn writer_thread(record_rx: Receiver<Record>) -> Result<()> {
    use ciborium::ser::into_writer as cborize;

    let mut num_record = 0u64;

    while let Ok(rec) = record_rx.recv() {
        let mut w = BufWriter::new(std::fs::File::create(format!(
            "cbor_fun/{num_record:05}.cbor"
        ))?);
        cborize(&rec, &mut w)?;
        w.flush()?;
        num_record += 1;
        if num_record >= 10000 {
            break;
        }
    }
    info!("{num_record:05} records processed");

    Ok(())
}

fn main() {
    run().unwrap_or_else(|e| {
        log::error!("{:?}", e);
        std::process::exit(1);
    });
}

/// Set up simplelog to spit messages to stderr.
fn init_logger(args: &Args) {
    let mut builder = ConfigBuilder::new();
    builder.set_target_level(LevelFilter::Off);
    builder.set_thread_level(LevelFilter::Off);
    builder.set_time_level(LevelFilter::Off);

    let level = match args.verbose {
        0 => LevelFilter::Warn,
        1 => LevelFilter::Info,
        2 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };

    if level == LevelFilter::Trace {
        builder.set_location_level(LevelFilter::Error);
    }
    builder.set_level_padding(LevelPadding::Left);

    let config = builder.build();

    let color = match args.color {
        Color::Always => ColorChoice::AlwaysAnsi,
        Color::Auto => {
            if atty::is(atty::Stream::Stderr) {
                ColorChoice::Auto
            } else {
                ColorChoice::Never
            }
        }
        Color::Never => ColorChoice::Never,
    };

    TermLogger::init(level, config.clone(), TerminalMode::Stderr, color)
        .or_else(|_| SimpleLogger::init(level, config))
        .context("Couldn't init logger")
        .unwrap()
}
