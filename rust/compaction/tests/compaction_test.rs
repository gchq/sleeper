use std::{collections::HashMap, fs::File, path::PathBuf, sync::Arc};

use arrow::{
    array::{Int32Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use compaction::{merge_sorted_files, ColRange, CompactionInput, PartitionBound};
use datafusion::parquet::{
    arrow::{arrow_reader::ArrowReaderBuilder, ArrowWriter},
    basic::Compression,
    file::properties::WriterProperties,
};
use tempfile::tempdir;
use url::Url;

#[tokio::test]
async fn should_merge_two_files() {
    // Given
    let dir = tempdir().unwrap();
    let file_1 = dir.path().join("file1.parquet");
    let file_2 = dir.path().join("file2.parquet");
    let output = dir.path().join("output.parquet");
    write_file_of_ints(&file_1, "key", Int32Array::from(vec![1, 3]));
    write_file_of_ints(&file_2, "key", Int32Array::from(vec![2, 4]));

    let input = CompactionInput {
        input_files: Vec::from([
            Url::from_file_path(file_1).unwrap(),
            Url::from_file_path(file_2).unwrap(),
        ]),
        output_file: Url::from_file_path(&output).unwrap(),
        row_key_cols: Vec::from(["key".to_string()]),
        region: HashMap::from([(
            "key".to_string(),
            ColRange {
                lower: PartitionBound::Int32(0),
                lower_inclusive: true,
                upper: PartitionBound::Int32(5),
                upper_inclusive: false,
            },
        )]),
        ..Default::default()
    };

    // When
    let result = merge_sorted_files(&input).await.unwrap();

    // Then
    assert_eq!(
        read_file_of_ints(&output),
        Int32Array::from(vec![1, 2, 3, 4])
    );
    assert_eq!([result.rows_read, result.rows_written], [4, 4]);
}

fn write_file_of_ints(file: &PathBuf, field_name: impl Into<String>, data: Int32Array) {
    let schema = Schema::new(vec![Field::new(field_name, DataType::Int32, false)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(data)]).unwrap();
    let props = WriterProperties::builder()
        .set_writer_version(datafusion::parquet::file::properties::WriterVersion::PARQUET_2_0)
        .set_compression(Compression::ZSTD(Default::default()))
        .set_column_index_truncate_length(Some(128))
        .set_statistics_truncate_length(Some(2147483647))
        .set_dictionary_enabled(true)
        .build();
    let f = File::create_new(file).unwrap();

    let mut writer = ArrowWriter::try_new(f, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).expect("Writing batch");
    writer.close().unwrap();
}

fn read_file_of_ints(path: &PathBuf) -> Int32Array {
    let file = File::open(path).unwrap();
    let batch = ArrowReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .to_owned()
}
