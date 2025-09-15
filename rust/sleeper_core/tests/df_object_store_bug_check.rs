//! This code presents a minimal working example to demonstrate the bug detailed
//! here [ListingTable and FileScanConfig assume all files accessible via single ObjectStore instance](https://github.com/apache/datafusion/issues/15964)
/*
 * Copyright 2022-2025 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use arrow::{
    array::{ArrayRef, Int32Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use color_eyre::eyre::Result;
use datafusion::{
    parquet::arrow::{AsyncArrowWriter, async_writer::ParquetObjectWriter},
    prelude::{ParquetReadOptions, SessionContext},
};
use object_store::{ObjectStore, memory::InMemory, path::Path};
use std::sync::Arc;
use url::Url;

/// This is example code to demonstrate a bug in `DataFusion`. If the bug gets fixed,
/// this test should start failing.
#[tokio::test]
async fn should_fail_on_datafusion_bug() -> Result<()> {
    let ctx = SessionContext::new();
    // Create a store and put a Parquet file into it
    let store1 = Arc::new(InMemory::new());
    write_some_parquet("test_path_1.parquet", store1.clone()).await?;
    // Tell DataFusion about the store
    ctx.register_object_store(&Url::parse("mem1://memory/")?, store1);

    // Make another store, in reality this could be another bucket on AWS S3.
    let store2 = Arc::new(InMemory::new());
    write_some_parquet("test_path_2.parquet", store2.clone()).await?;
    ctx.register_object_store(&Url::parse("mem2://memory/")?, store2);

    // Now read the data from store 1 to confirm it works
    let frame = ctx
        .read_parquet(
            vec!["mem1://memory/test_path_1.parquet"],
            ParquetReadOptions::default(),
        )
        .await?;
    let mem_read = frame.show().await;
    assert!(mem_read.is_ok());

    // Now read the data from store 2 to confirm it works
    let frame = ctx
        .read_parquet(
            vec!["mem2://memory/test_path_2.parquet"],
            ParquetReadOptions::default(),
        )
        .await?;
    let mem_read = frame.show().await;
    assert!(mem_read.is_ok());

    // TRIGGER BUG - let's read from both
    // Expected behaviour - frame should contain data from both files
    // Actual behaviour - operation fails with error similar to:
    // Object at location test_path_2.parquet not found
    // Bug appears due to ListingTable and FileScanConfig appear to assume all files can be located
    // in first object store.
    //
    // If the second path is altered to "mem2://memory/test_path1.parquet" the read succeeds, even though
    // that ObjectStore doesn't contain a file by that name!
    let frame = ctx
        .read_parquet(
            vec![
                "mem1://memory/test_path_1.parquet",
                "mem2://memory/test_path_2.parquet",
            ],
            ParquetReadOptions::default(),
        )
        .await?;
    let mem_read = frame.show().await;

    assert!(mem_read.is_err());

    Ok(())
}

async fn write_some_parquet(file_name: &str, store: Arc<dyn ObjectStore>) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key1", DataType::Int32, false),
        Field::new("key2", DataType::Int32, false),
    ]));
    let data_1 = batch_of_int_fields(schema.clone(), [vec![1, 2, 3], vec![11, 12, 13]])?;

    let object_store_writer = ParquetObjectWriter::new(store.clone(), Path::from(file_name));
    let mut writer = AsyncArrowWriter::try_new(object_store_writer, data_1.schema(), None).unwrap();
    writer.write(&data_1).await?;
    writer.close().await?;
    Ok(())
}

#[allow(clippy::missing_errors_doc)]
fn batch_of_int_fields<const N: usize>(
    schema: Arc<Schema>,
    fields_data: [Vec<i32>; N],
) -> Result<RecordBatch> {
    let columns: Vec<ArrayRef> = fields_data
        .into_iter()
        .map(|field_data| Arc::new(Int32Array::from(field_data)) as ArrayRef)
        .collect();
    Ok(RecordBatch::try_new(schema, columns)?)
}
