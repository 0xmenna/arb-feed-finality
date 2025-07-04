use csv::Writer;
use serde::Serialize;
use std::fs::OpenOptions;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::Mutex;

const DEFAULT_BENCHMARK_FILE: &str = "benchmark.csv";

pub const UNIT_TX_SIZE: usize = 120;

#[derive(Default)]
pub struct BenchmarkData {
    pub checkpoint: u64,
    pub round: u64,
    pub finalization_ms: u128,
    pub block_msg_size: usize,
    pub block_batch_size: usize,
}

/// A single benchmark entry
#[derive(Serialize)]
struct BenchmarkRecord {
    checkpoint: u64,
    round: u64,
    finalization_ms: u128,
    tx_count: u64,
    batch_size: usize,
}

pub struct BenchLogger {
    path: PathBuf,
    flushing_data: BenchmarkData,
    writer: Writer<BufWriter<std::fs::File>>,
}

impl BenchLogger {
    /// Creates a new logger or opens an existing one
    pub fn new() -> std::io::Result<Self> {
        let path: PathBuf = DEFAULT_BENCHMARK_FILE.into();

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true) // <-- this ensures overwrite
            .open(&path)?;

        let mut writer = Writer::from_writer(BufWriter::new(file));

        // Always write headers since we recreated the file
        writer.write_record(&[
            "checkpoint",
            "round",
            "finalization_ms",
            "tx_count",
            "batch_size",
        ])?;
        writer.flush()?;

        Ok(BenchLogger {
            path,
            flushing_data: BenchmarkData::default(),
            writer,
        })
    }

    pub fn bench_data(&self) -> &BenchmarkData {
        &self.flushing_data
    }

    pub fn set_data_at_proposal(
        &mut self,
        checkpoint: u64,
        round: u64,
        block_msg_size: usize,
        block_batch_size: usize,
    ) {
        self.flushing_data.checkpoint = checkpoint;
        self.flushing_data.round = round;
        self.flushing_data.block_msg_size = block_msg_size;
        self.flushing_data.block_batch_size = block_batch_size;
    }

    pub fn set_data_at_finalization(&mut self, finalization_ms: u128) {
        self.flushing_data.finalization_ms = finalization_ms;
    }

    /// Logs a new benchmark record
    pub fn log(&mut self) -> std::io::Result<()> {
        let record: BenchmarkRecord = BenchmarkRecord {
            checkpoint: self.flushing_data.checkpoint,
            round: self.flushing_data.round,
            finalization_ms: self.flushing_data.finalization_ms,
            tx_count: ((self.flushing_data.block_msg_size + UNIT_TX_SIZE - 1) / UNIT_TX_SIZE)
                as u64,
            batch_size: self.flushing_data.block_batch_size,
        };

        self.writer.serialize(record)?;
        self.writer.flush()?;
        Ok(())
    }
}
