use std::{
    fs::File,
    io::{self, prelude::*, BufReader, BufWriter},
};

use anyhow::{bail, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use clap::Parser;
use float_ord::FloatOrd;
use log::*;
use rustc_hash::FxHashMap;
use simplelog::*;
use tacview::{
    record::{Coords, GlobalProperty, Property, Record, Update},
    ParseError,
};

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

#[derive(Debug, Copy, Clone, Default)]
struct LL {
    lat: f64,
    lon: f64,
}

fn run() -> Result<()> {
    let args = Args::parse();
    init_logger(&args);

    if args.acmi == "-" {
        bail!("Reading from stdin currently unsupported (can't seek that!)");
    }

    let mut fh = File::open(&args.acmi).context("Couldn't open ACMI")?;

    let mut reader = Reader::new(&args.acmi, &mut fh)?;

    let mut reference_ll: LL = LL::default();

    for global in &mut reader {
        let global = match global? {
            Record::GlobalProperty(gp) => gp,
            _ => break,
        };

        match global {
            GlobalProperty::ReferenceLatitude(lat) => {
                reference_ll.lat = lat;
            }
            GlobalProperty::ReferenceLongitude(lon) => {
                reference_ll.lon = lon;
            }
            _not_ll => {}
        }
    }
    debug!("Original reference lat/lon: {reference_ll:?}");

    let min_ll = find_min_ll(reader)?;
    debug!("Min lat/lon: {min_ll:?}");
    let new_reference_ll = LL {
        lat: (reference_ll.lat + min_ll.lat).floor(),
        lon: (reference_ll.lon + min_ll.lon).floor(),
    };
    debug!("New reference lat/lon: {new_reference_ll:?}");

    let mut last_frame = 0f64;
    let mut active_entities: FxHashMap<u64, Vec<Property>> = FxHashMap::default();

    fh.rewind()?;
    let reader = Reader::new(&args.acmi, &mut fh)?;

    let mut w = tacview::Writer::new(BufWriter::new(io::stdout().lock()))?;

    for rec in reader {
        match rec? {
            // Pass global properties through, except lat/lon.
            // Change those to the new one!
            Record::GlobalProperty(gp) => match gp {
                GlobalProperty::ReferenceLatitude(_) => {
                    w.write(GlobalProperty::ReferenceLatitude(new_reference_ll.lat))?;
                }
                GlobalProperty::ReferenceLongitude(_) => {
                    w.write(GlobalProperty::ReferenceLongitude(new_reference_ll.lon))?;
                }
                not_ll => w.write(not_ll)?,
            },

            // Pass events through unscathed
            Record::Event(e) => w.write(Record::Event(e))?,

            // Pass frame timestamps through only when they're new.
            Record::Frame(ts) => {
                if ts != last_frame {
                    w.write(Record::Frame(ts))?;
                    last_frame = ts;
                }
            }

            Record::Update(mut up) => {
                // Fix up coords.
                for prop in &mut up.props {
                    match prop {
                        Property::T(c) => {
                            offset_coords(c, &reference_ll, &new_reference_ll);
                        }
                        _ => {}
                    }
                }

                use std::collections::hash_map::Entry;

                // Horrid:
                match active_entities.entry(up.id) {
                    Entry::Vacant(v) => {
                        v.insert(up.props.clone());
                        w.write(up)?;
                    },
                    Entry::Occupied(mut o) => {
                        if up.props != *o.get() {
                            // TODO: Only write fields that changed!
                            *o.get_mut() = up.props.clone();
                            w.write(up)?;
                        }
                    }
                }
            }

            // Pass removals through if they're something we're tracking.
            Record::Remove(id) => {
                if active_entities.remove(&id).is_some() {
                    w.write(Record::Remove(id))?;
                }
            }
        };
    }

    Ok(())
}

fn offset_coords(c: &mut Coords, old_ref: &LL, new_ref: &LL) {
    if let Some(lat) = &mut c.latitude {
        *lat += old_ref.lat;
        *lat -= new_ref.lat;
        assert!(*lat > 0.0);
    }
    if let Some(lon) = &mut c.longitude {
        *lon += old_ref.lon;
        *lon -= new_ref.lon;
        assert!(*lon > 0.0);
    }
}

fn find_min_ll(records: Reader) -> Result<LL> {
    let mut new_ref_lat = None;
    let mut new_ref_lon = None;

    for rec in records {
        match rec? {
            Record::Update(Update { props, .. }) => {
                if let Some(coords) = props.iter().find(|p| matches!(p, Property::T(_))) {
                    let coords = match coords {
                        Property::T(t) => t,
                        _ => unreachable!(),
                    };

                    if let Some(lat) = coords.latitude {
                        new_ref_lat = Some(match new_ref_lat {
                            None => lat,
                            Some(prev) => std::cmp::min(FloatOrd(prev), FloatOrd(lat)).0,
                        });
                    }
                    if let Some(lon) = coords.longitude {
                        new_ref_lon = Some(match new_ref_lon {
                            None => lon,
                            Some(prev) => std::cmp::min(FloatOrd(prev), FloatOrd(lon)).0,
                        });
                    }
                }
            }
            _ => {}
        }
    }

    Ok(LL {
        lat: new_ref_lat.unwrap_or(0f64),
        lon: new_ref_lon.unwrap_or(0f64),
    })
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
