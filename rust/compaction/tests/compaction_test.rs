use std::{collections::HashMap, fs::File, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, Int32Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use color_eyre::eyre::Error;
use compaction::{merge_sorted_files, ColRange, CompactionInput, PartitionBound};
use datafusion::parquet::{
    arrow::{arrow_reader::ArrowReaderBuilder, ArrowWriter},
    basic::Compression,
    file::properties::WriterProperties,
};
use tempfile::{tempdir, TempDir};
use url::Url;

#[tokio::test]
async fn should_merge_two_files() -> Result<(), Error> {
    // Given
    let dir = tempdir()?;
    let file_1 = file(&dir, "file1.parquet");
    let file_2 = file(&dir, "file2.parquet");
    let output = file(&dir, "output.parquet");
    write_file_of_ints(&file_1, "key", vec![1, 3])?;
    write_file_of_ints(&file_2, "key", vec![2, 4])?;

    let input = CompactionInput {
        input_files: Vec::from([file_1, file_2]),
        output_file: output.clone(),
        row_key_cols: row_key_cols(["key"]),
        region: single_int_range("key", 0, 5),
        ..Default::default()
    };

    // When
    let result = merge_sorted_files(&input).await?;

    // Then
    assert_eq!(read_file_of_ints(&output, "key")?, vec![1, 2, 3, 4]);
    assert_eq!([result.rows_read, result.rows_written], [4, 4]);
    Ok(())
}

#[tokio::test]
async fn should_merge_files_with_overlapping_data() -> Result<(), Error> {
    // Given
    let dir = tempdir()?;
    let file_1 = file(&dir, "file1.parquet");
    let file_2 = file(&dir, "file2.parquet");
    let output = file(&dir, "output.parquet");
    write_file_of_ints(&file_1, "key", vec![1, 2])?;
    write_file_of_ints(&file_2, "key", vec![2, 3])?;

    let input = CompactionInput {
        input_files: Vec::from([file_1, file_2]),
        output_file: output.clone(),
        row_key_cols: row_key_cols(["key"]),
        region: single_int_range("key", 0, 5),
        ..Default::default()
    };

    // When
    let result = merge_sorted_files(&input).await?;

    // Then
    assert_eq!(read_file_of_ints(&output, "key")?, vec![1, 2, 2, 3]);
    assert_eq!([result.rows_read, result.rows_written], [4, 4]);
    Ok(())
}

#[tokio::test]
async fn should_exclude_data_not_in_region() -> Result<(), Error> {
    // Given
    let dir = tempdir()?;
    let file_1 = file(&dir, "file1.parquet");
    let file_2 = file(&dir, "file2.parquet");
    let output = file(&dir, "output.parquet");
    write_file_of_ints(&file_1, "key", vec![1, 2])?;
    write_file_of_ints(&file_2, "key", vec![3, 4])?;

    let input = CompactionInput {
        input_files: Vec::from([file_1, file_2]),
        output_file: output.clone(),
        row_key_cols: row_key_cols(["key"]),
        region: single_int_range("key", 2, 4),
        ..Default::default()
    };

    // When
    let result = merge_sorted_files(&input).await?;

    // Then
    assert_eq!(read_file_of_ints(&output, "key")?, vec![2, 3]);
    assert_eq!([result.rows_read, result.rows_written], [2, 2]);
    Ok(())
}

#[tokio::test]
async fn should_exclude_data_not_in_multidimensional_region() -> Result<(), Error> {
    // Given
    let dir = tempdir()?;
    let file_1 = file(&dir, "file1.parquet");
    let file_2 = file(&dir, "file2.parquet");
    let output = file(&dir, "output.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("key1", DataType::Int32, false),
        Field::new("key2", DataType::Int32, false),
    ]));
    let data_1 = batch_of_int_fields(schema.clone(), [vec![1, 2, 3], vec![11, 12, 13]])?;
    let data_2 = batch_of_int_fields(schema.clone(), [vec![2, 3, 4], vec![22, 23, 24]])?;
    write_file(&file_1, data_1)?;
    write_file(&file_2, data_2)?;

    let input = CompactionInput {
        input_files: Vec::from([file_1, file_2]),
        output_file: output.clone(),
        row_key_cols: row_key_cols(["key1", "key2"]),
        region: HashMap::from([
            region_entry("key1", int_range(2, 4)),
            region_entry("key2", int_range(13, 23)),
        ]),
        ..Default::default()
    };

    // When
    let result = merge_sorted_files(&input).await?;

    // Then
    assert_eq!(
        read_file_of_int_fields(&output, ["key1", "key2"])?,
        vec![[2, 22], [3, 13]]
    );
    assert_eq!([result.rows_read, result.rows_written], [2, 2]);
    Ok(())
}

fn file(dir: &TempDir, name: &str) -> Url {
    Url::from_file_path(dir.path().join(name)).unwrap()
}

fn row_key_cols<const N: usize>(names: [&str; N]) -> Vec<String> {
    names.into_iter().map(String::from).collect()
}

fn write_file_of_ints(path: &Url, field_name: &str, data: Vec<i32>) -> Result<(), Error> {
    let schema = Schema::new(vec![Field::new(field_name, DataType::Int32, false)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Int32Array::from(data))])?;
    write_file(path, batch)
}

fn write_file(path: &Url, batch: RecordBatch) -> Result<(), Error> {
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

fn batch_of_int_fields<const N: usize>(
    schema: Arc<Schema>,
    fields_data: [Vec<i32>; N],
) -> Result<RecordBatch, Error> {
    let columns: Vec<ArrayRef> = fields_data
        .into_iter()
        .map(|field_data| Arc::new(Int32Array::from(field_data)) as ArrayRef)
        .collect();
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn read_file_of_ints(path: &Url, field_name: &str) -> Result<Vec<i32>, Error> {
    let file = File::open(path.path())?;
    let mut data: Vec<i32> = Vec::new();
    for result in ArrowReaderBuilder::try_new(file)?.build()? {
        data.extend(get_int_array(field_name, &result?)?.values());
    }
    Ok(data)
}

fn read_file_of_int_fields<const N: usize>(
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

fn single_int_range(field_name: &str, min: i32, max: i32) -> HashMap<String, ColRange<'_>> {
    HashMap::from([region_entry(field_name, int_range(min, max))])
}

fn region_entry<'r>(field_name: &str, range: ColRange<'r>) -> (String, ColRange<'r>) {
    (String::from(field_name), range)
}

fn int_range<'r>(min: i32, max: i32) -> ColRange<'r> {
    ColRange {
        lower: PartitionBound::Int32(min),
        lower_inclusive: true,
        upper: PartitionBound::Int32(max),
        upper_inclusive: false,
    }
}
