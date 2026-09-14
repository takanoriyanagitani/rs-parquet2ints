use std::io;
use std::process::ExitCode;

use rs_parquet2ints::Cfg;
use rs_parquet2ints::Endian;

fn io_filename() -> impl Fn() -> String {
    || std::env::var("ENV_PARQUET_NAME").unwrap_or_default()
}

fn io_colix() -> impl Fn() -> usize {
    || {
        std::env::var("ENV_COL_INDEX")
            .ok()
            .and_then(|s| str::parse(&s).ok())
            .unwrap_or_default()
    }
}

fn io_batsz() -> impl Fn() -> usize {
    || {
        std::env::var("ENV_BATCH_SIZE")
            .ok()
            .and_then(|s| str::parse(&s).ok())
            .unwrap_or(8192)
    }
}

fn io_endia() -> impl Fn() -> Endian {
    || {
        std::env::var("ENV_ENDIAN")
            .ok()
            .and_then(|s| str::parse(&s).ok())
            .unwrap_or_default()
    }
}

fn io_cfg() -> impl Fn() -> Cfg {
    || {
        let filename: String = io_filename()();
        let columnix: usize = io_colix()();
        let batch_sz: usize = io_batsz()();
        let endian4i: Endian = io_endia()();
        Cfg {
            filename,
            columnix,
            batch_sz,
            endian4i,
        }
    }
}

fn sub() -> Result<(), io::Error> {
    let cfg: Cfg = io_cfg()();
    cfg.pfile2brdr2stdout()
}

fn main() -> ExitCode {
    sub().map(|_| ExitCode::SUCCESS).unwrap_or_else(|e| {
        eprintln!("{e}");
        let cfg: Cfg = io_cfg()();
        let filename: String = cfg.filename;
        let columnix: usize = cfg.columnix;
        let batch_sz: usize = cfg.batch_sz;
        let endian4i: Endian = cfg.endian4i;
        eprintln!("ENV_PARQUET_NAME: {filename}");
        eprintln!("ENV_COL_INDEX: {columnix}");
        eprintln!("ENV_BATCH_SIZE: {batch_sz}");
        eprintln!("ENV_ENDIAN: {endian4i:#?}");
        ExitCode::FAILURE
    })
}
