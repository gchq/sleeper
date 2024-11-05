use std::{collections::HashMap, fs::File, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, Int32Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use color_eyre::eyre::Error;
use compaction::{ColRange, PartitionBound};
use datafusion::parquet::{
    arrow::{arrow_reader::ArrowReaderBuilder, ArrowWriter},
    basic::Compression,
    file::properties::WriterProperties,
};
use tempfile::TempDir;
use url::Url;

pub fn file(dir: &TempDir, name: &str) -> Url {
    Url::from_file_path(dir.path().join(name)).unwrap()
}

pub fn row_key_cols<const N: usize>(names: [&str; N]) -> Vec<String> {
    names.into_iter().map(String::from).collect()
}

pub fn write_file_of_ints(path: &Url, field_name: &str, data: Vec<i32>) -> Result<(), Error> {
    let schema = Schema::new(vec![Field::new(field_name, DataType::Int32, false)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Int32Array::from(data))])?;
    write_file(path, batch)
}

pub fn write_file(path: &Url, batch: RecordBatch) -> Result<(), Error> {
    let file = File::create_new(path.path())?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), writer_props())?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn writer_props() -> Option<WriterProperties> {
    Some(
        WriterProperties::builder()
            .set_writer_version(datafusion::parquet::file::properties::WriterVersion::PARQUET_2_0)
            .set_compression(Compression::ZSTD(Default::default()))
            .set_column_index_truncate_length(Some(128))
            .set_statistics_truncate_length(Some(2147483647))
            .set_dictionary_enabled(true)
            .build(),
    )
}

pub fn batch_of_int_fields<const N: usize>(
    schema: Arc<Schema>,
    fields_data: [Vec<i32>; N],
) -> Result<RecordBatch, Error> {
    let columns: Vec<ArrayRef> = fields_data
        .into_iter()
        .map(|field_data| Arc::new(Int32Array::from(field_data)) as ArrayRef)
        .collect();
    Ok(RecordBatch::try_new(schema, columns)?)
}

pub fn read_file_of_ints(path: &Url, field_name: &str) -> Result<Vec<i32>, Error> {
    let file = File::open(path.path())?;
    let mut data: Vec<i32> = Vec::new();
    for result in ArrowReaderBuilder::try_new(file)?.build()? {
        data.extend(get_int_array(field_name, &result?)?.values());
    }
    Ok(data)
}

pub fn read_file_of_int_fields<const N: usize>(
    path: &Url,
    field_names: [&str; N],
) -> Result<Vec<[i32; N]>, Error> {
    let file = File::open(path.path())?;
    let mut data: Vec<[i32; N]> = Vec::new();
    for result in ArrowReaderBuilder::try_new(file)?.build()? {
        let batch = result?;
        let arrays: Vec<&Int32Array> = get_int_arrays(&batch, field_names)?;
        data.extend((0..batch.num_rows()).map(|row_number| read_row(row_number, &arrays)));
    }
    Ok(data)
}

fn get_int_arrays<'b, const N: usize>(
    batch: &'b RecordBatch,
    field_names: [&str; N],
) -> Result<Vec<&'b Int32Array>, Error> {
    let result: Result<Vec<&Int32Array>, Error> = field_names
        .iter()
        .map(|field_name| get_int_array(field_name, &batch))
        .collect();
    Ok(result?)
}

fn read_row<const N: usize>(row_number: usize, arrays: &Vec<&Int32Array>) -> [i32; N] {
    let mut row = [0; N];
    for i in 0..N {
        row[i] = arrays.get(i).unwrap().value(row_number);
    }
    row
}

fn get_int_array<'b>(field_name: &str, batch: &'b RecordBatch) -> Result<&'b Int32Array, Error> {
    Ok(batch
        .column_by_name(field_name)
        .ok_or_else(|| Error::msg("field not found"))?
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| Error::msg("could not read field as an integer"))?)
}

pub fn single_int_range(field_name: &str, min: i32, max: i32) -> HashMap<String, ColRange<'_>> {
    HashMap::from([region_entry(field_name, int_range(min, max))])
}

pub fn region_entry<'r>(field_name: &str, range: ColRange<'r>) -> (String, ColRange<'r>) {
    (String::from(field_name), range)
}

pub fn int_range<'r>(min: i32, max: i32) -> ColRange<'r> {
    ColRange {
        lower: PartitionBound::Int32(min),
        lower_inclusive: true,
        upper: PartitionBound::Int32(max),
        upper_inclusive: false,
    }
}
