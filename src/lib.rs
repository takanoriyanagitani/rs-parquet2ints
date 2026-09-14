use core::any::Any;

use std::io;

use std::fs::File;

use io::BufWriter;
use io::Write;

use arrow_array::ArrayRef;
use arrow_array::RecordBatch;
use arrow_array::array::Int32Array;

use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::schema::types::SchemaDescriptor;

#[derive(Default, Debug, Clone, Copy)]
pub enum Endian {
    #[default]
    Lit,
    Big,
}

impl std::str::FromStr for Endian {
    type Err = io::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "l" => Ok(Self::Lit),
            "lit" => Ok(Self::Lit),
            "Lit" => Ok(Self::Lit),
            "little" => Ok(Self::Lit),
            "Little" => Ok(Self::Lit),
            "b" => Ok(Self::Big),
            "big" => Ok(Self::Big),
            "Big" => Ok(Self::Big),
            _ => Err(io::Error::other("invalid endian string")),
        }
    }
}

impl Endian {
    pub fn wtr2int_consumer<W>(&self, mut wtr: W) -> impl FnMut(i32) -> Result<(), io::Error>
    where
        W: Write,
    {
        let i2b = match self {
            Self::Lit => |i: i32| i.to_le_bytes(),
            Self::Big => |i: i32| i.to_be_bytes(),
        };

        move |i: i32| {
            let b: [u8; 4] = i2b(i);
            wtr.write_all(&b)
        }
    }
}

pub struct ProjMask(pub ProjectionMask);

impl ProjMask {
    pub fn new(colix: usize, sdesc: &SchemaDescriptor) -> Self {
        Self(ProjectionMask::leaves(sdesc, vec![colix]))
    }
}

pub struct ArrowBatchReader(pub ParquetRecordBatchReader);

impl ArrowBatchReader {
    pub fn into_batches(self) -> impl Iterator<Item = Result<RecordBatch, io::Error>> {
        self.0.map(|r| r.map_err(io::Error::other))
    }
}

pub struct Batch(pub RecordBatch);

impl Batch {
    pub fn to_sink<S>(&self, sink: &mut S, colix: usize) -> Result<(), io::Error>
    where
        S: FnMut(i32) -> Result<(), io::Error>,
    {
        let col: &ArrayRef = self.0.column(colix);
        let acol: &dyn Any = col.as_any();
        let ocol: Option<&Int32Array> = acol.downcast_ref::<Int32Array>();
        let Some(i3a) = ocol else {
            return Err(io::Error::other("not an i32 column"));
        };
        let sbuf = i3a.values();
        let si: &[i32] = sbuf;
        for i in si {
            sink(*i)?;
        }
        Ok(())
    }
}

impl ArrowBatchReader {
    pub fn into_sink<S>(self, colix: usize, mut sink: S) -> Result<(), io::Error>
    where
        S: FnMut(i32) -> Result<(), io::Error>,
    {
        let irbat = self.into_batches();
        for rslt in irbat {
            let rbat: RecordBatch = rslt?;
            Batch(rbat).to_sink(&mut sink, colix)?;
        }
        Ok(())
    }
}

pub struct ParquetFile(pub File);

impl ParquetFile {
    pub fn into_builder(self) -> Result<ParquetRecordBatchReaderBuilder<File>, io::Error> {
        ParquetRecordBatchReaderBuilder::try_new(self.0).map_err(io::Error::other)
    }
}

pub struct Cfg {
    pub filename: String,
    pub columnix: usize,
    pub batch_sz: usize,
    pub endian4i: Endian,
}

impl Cfg {
    pub fn to_reader(&self) -> Result<ParquetRecordBatchReader, io::Error> {
        let f: File = File::open(&self.filename)?;
        let bldr: ParquetRecordBatchReaderBuilder<_> = ParquetFile(f).into_builder()?;
        let sch: &SchemaDescriptor = bldr.parquet_schema();
        let msk: ProjMask = ProjMask::new(self.columnix, sch);
        bldr.with_projection(msk.0)
            .with_batch_size(self.batch_sz)
            .build()
            .map_err(io::Error::other)
    }
}

impl Cfg {
    pub fn pfile2brdr2sink<S>(&self, sink: S) -> Result<(), io::Error>
    where
        S: FnMut(i32) -> Result<(), io::Error>,
    {
        let rdr: ParquetRecordBatchReader = self.to_reader()?;
        let abr = ArrowBatchReader(rdr);
        abr.into_sink(0, sink)
    }
}

impl Cfg {
    pub fn pfile2brdr2wtr<W>(&self, mut wtr: W) -> Result<(), io::Error>
    where
        W: Write,
    {
        let sink = self.endian4i.wtr2int_consumer(&mut wtr);
        self.pfile2brdr2sink(sink)?;
        wtr.flush()
    }
}

impl Cfg {
    pub fn pfile2brdr2stdout(&self) -> Result<(), io::Error> {
        let o = io::stdout();
        let mut ol = o.lock();
        self.pfile2brdr2wtr(BufWriter::new(&mut ol))?;
        ol.flush()
    }
}
