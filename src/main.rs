use std::{
    fs::File,
    io::{self, prelude::*, BufReader, BufWriter},
    mem::{discriminant, Discriminant},
};

use anyhow::{bail, Context, Result};
use bytesize::ByteSize;
use camino::{Utf8Path, Utf8PathBuf};
use clap::Parser;
use crossbeam::channel::{bounded, Receiver};
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

struct CountingWriter<W> {
    inner: W,
    written: u64,
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.written += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<W: Write> CountingWriter<W> {
    fn new(inner: W) -> Self {
        Self { inner, written: 0 }
    }
}

#[derive(Debug, Copy, Clone, Default)]
struct LL {
    lat: f64,
    lon: f64,
}

type PropertyMap = FxHashMap<Discriminant<Property>, Property>;

fn run() -> Result<()> {
    let args = Args::parse();
    init_logger(&args);

    if args.acmi == "-" {
        bail!("Reading from stdin currently unsupported (can't seek that!)");
    }

    let mut fh = File::open(&args.acmi).context("Couldn't open ACMI")?;

    let mut reader = Reader::new(&args.acmi, &mut fh)?;

    let reference_ll = parse_original_ll(&mut reader)?;
    debug!("Original reference lat/lon: {reference_ll:?}");

    let min_ll = find_min_ll(reader)?;
    debug!("Min lat/lon: {min_ll:?}");
    let new_reference_ll = LL {
        lat: (reference_ll.lat + min_ll.lat).floor(),
        lon: (reference_ll.lon + min_ll.lon).floor(),
    };
    debug!("New reference lat/lon: {new_reference_ll:?}");

    let original_size = fh.stream_position()?;
    fh.rewind()?;

    let (tx, rx) = bounded(1024);

    std::thread::scope(|s| {
        let write_thread =
            s.spawn(move || writer_thread(rx, &reference_ll, &new_reference_ll, original_size));

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

fn writer_thread(
    record_rx: Receiver<Record>,
    reference_ll: &LL,
    new_reference_ll: &LL,
    original_size: u64,
) -> Result<()> {
    let mut w = tacview::Writer::new(CountingWriter::new(BufWriter::new(io::stdout().lock())))?;

    let mut this_frame = 0f64;
    let mut active_entities: FxHashMap<u64, PropertyMap> = FxHashMap::default();

    // Dumb experiment
    let mut total_coords = 0u64;
    let mut total_props = 0u64;

    info!("Rewriting all records");
    while let Ok(rec) = record_rx.recv() {
        match rec {
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
                if ts != this_frame {
                    w.write(Record::Frame(ts))?;
                    this_frame = ts;
                } else {
                    trace!("Skipping redundant frame time {ts:.2}");
                }
            }

            Record::Update(mut up) => {
                // Fix up coords.
                for prop in &mut up.props {
                    if let Property::T(c) = prop {
                        offset_coords(c, &reference_ll, &new_reference_ll);
                        total_coords += 1;
                    }
                    total_props += 1;
                }

                let props = props_map(up.props);

                use std::collections::hash_map::Entry;

                match active_entities.entry(up.id) {
                    Entry::Vacant(v) => {
                        // Back into a list you go
                        // (change tacview-rs to store maps?)
                        up.props = props.values().cloned().collect();
                        w.write(up)?;

                        v.insert(props);
                    }
                    Entry::Occupied(mut o) => {
                        let mut changed_props = PropertyMap::default();

                        // For each property in the new update,
                        for (prop_type, prop) in props {
                            // If we were already tracking that property and it changed,
                            // note that.
                            if let Some(prev) = o.get().get(&prop_type) {
                                // Coords are a speical case:
                                // Like properties, individiual entries in them
                                // can be left blank if they haven't changed.
                                if let Property::T(prev_coord) = prev {
                                    let curr_coord = match prop {
                                        Property::T(c) => c,
                                        _ => unreachable!(),
                                    };

                                    let delta = prev_coord.delta(&curr_coord);

                                    // If any fields changed, _then_ we care.
                                    if delta != Coords::default() {
                                        changed_props.insert(prop_type, Property::T(delta));
                                    }
                                } else if *prev != prop {
                                    changed_props.insert(prop_type, prop);
                                }
                            }
                            // And if we weren't tracking that property yet, note that.
                            else {
                                changed_props.insert(prop_type, prop);
                            }
                        }

                        if !changed_props.is_empty() {
                            // We only need to record properties that changed:
                            w.write(Update {
                                id: up.id,
                                props: changed_props.values().cloned().collect(),
                            })?;

                            // And merge them back into our record
                            o.get_mut().extend(changed_props);
                        } else {
                            // trace!("No properties changed for {:x} at {}", up.id, this_frame);
                        }
                    }
                }
            }

            // Pass removals through if they're something we're tracking.
            Record::Remove(id) => {
                if active_entities.remove(&id).is_some() {
                    w.write(Record::Remove(id))?;
                } else {
                    trace!("Skipping redundant remove for {id:x}");
                }
            }
        };
    }

    let mut w = w.into_inner();
    w.flush()?;
    let compressed_size = w.written;

    info!(
        "Compressed {} ACMI to {} ({:.1}%)",
        ByteSize::b(original_size),
        ByteSize::b(compressed_size),
        compressed_size as f64 / original_size as f64 * 100.0
    );
    debug!(
        "{}/{} coords ({:.1}%)",
        total_coords,
        total_props,
        total_coords as f64 / total_props as f64 * 100.0
    );

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

fn props_map(props: Vec<Property>) -> PropertyMap {
    let mut map = PropertyMap::with_capacity_and_hasher(props.len(), Default::default());
    map.extend(props.into_iter().map(|p| (discriminant(&p), p)));
    map
}

fn parse_original_ll(reader: &mut Reader) -> Result<LL> {
    let mut reference_ll: LL = LL::default();

    for global in reader {
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

    Ok(reference_ll)
}

fn find_min_ll(records: Reader) -> Result<LL> {
    info!("Reading all records to find the minimum lat/lon");

    let mut new_ref_lat = None;
    let mut new_ref_lon = None;

    for rec in records {
        if let Record::Update(Update { props, .. }) = rec? {
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
