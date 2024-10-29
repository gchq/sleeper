use std::{collections::HashMap, fs::File, sync::Arc};

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
use tempfile::{tempdir, TempDir};
use url::Url;

#[tokio::test]
async fn should_merge_two_files() {
    // Given
    let dir = tempdir().unwrap();
    let file_1 = file(&dir, "file1.parquet");
    let file_2 = file(&dir, "file2.parquet");
    let output = file(&dir, "output.parquet");
    write_file_of_ints(&file_1, "key", Int32Array::from(vec![1, 3]));
    write_file_of_ints(&file_2, "key", Int32Array::from(vec![2, 4]));

    let input = CompactionInput {
        input_files: Vec::from([file_1, file_2]),
        output_file: output.clone(),
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
        read_file_of_ints(&output, "key"),
        Int32Array::from(vec![1, 2, 3, 4])
    );
    assert_eq!([result.rows_read, result.rows_written], [4, 4]);
}

fn file(dir: &TempDir, name: impl Into<String>) -> Url {
    Url::from_file_path(dir.path().join(name.into())).unwrap()
}

fn write_file_of_ints(path: &Url, field_name: &str, data: Int32Array) {
    let schema = Schema::new(vec![Field::new(field_name, DataType::Int32, false)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(data)]).unwrap();
    let props = WriterProperties::builder()
        .set_writer_version(datafusion::parquet::file::properties::WriterVersion::PARQUET_2_0)
        .set_compression(Compression::ZSTD(Default::default()))
        .set_column_index_truncate_length(Some(128))
        .set_statistics_truncate_length(Some(2147483647))
        .set_dictionary_enabled(true)
        .build();
    let file = File::create_new(path.path()).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).expect("Writing batch");
    writer.close().unwrap();
}

fn read_file_of_ints(path: &Url, field_name: &str) -> Int32Array {
    let file = File::open(path.path()).unwrap();
    let batch = ArrowReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    batch
        .column_by_name(field_name)
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .to_owned()
}
