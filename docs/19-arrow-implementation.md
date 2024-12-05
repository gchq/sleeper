## Arrow design

The arrow ingest design uses a batch buffer. This buffer is used for holding the records of a local file, the sleeper 
records are then writtent to this buffer.

If this buffer files up, the batcher with catch and OutOfMemoryException when the next record is added.
The whole bactch is then sorted, and once complete the full data is written to a local file. (see the below section for further information)

### Working buffer

The working buffer is used for a vector of indexes when the process is sorting the records. 1 integer is provided for 
each record. It is this buffer that is updated during sorting, meaning that the record data will remain unchanged.
The arrow record batches are then held within memory when attempting to write them to file, this memory is shared with
the memory ysed for the vector of indexes. This is done on a singular basis for the batches and each record batch is 
handled in turn from ArrowRecordBatch.sortArrowAndWriteToLocalFile.
The data from the batch buffer is copied over following the sort order from with the vector of indexes.

When attempted to read the record batchs from the local file, the allocator for the buffer is passed into the 
ArrowStreamReader. This in turn interacts with the RecordIteratorFromArrowStreamReader which will recieve the Arrow 
batches from the reader. During this, it will convert the Arrow batches in Sleeper records objects, so that the can then
be written to S3 via Parquet.

## Possible failure states:

### Once record doesn't fit into the whole batch buffer

This would occur if the record itself is very large in size (batch buffer size is defined by `sleeper.ingest.arrow.batch.buffer.bytes`)
An assertionError is thrown during the execution of flushing the fille to local. [ArrowRecordBatch.flushToLocalArrowFileThenClear]
Resolution for this would primarily be through expanding the buffer size if possible, alternatively adjust the inpuut to 
break into more managable chunks should allow avoidance of this issue.

### Vector of indexs doesn't fit in the working buffer
Likely to occur when the recors are small enough that when the batch buffer is full we would have too many requests.
For example if the record was a single integer, the vector to the indexes would likely be similar in length to that 
of the batch buffer. An OutOfMemoryException would be thrown at the end of the sorting of the Vector contents.
The likely resolution for this is to expand the the working buffer capacity.

### Arrow record batch doesn't fit in the working buffer
The batch size of the records is configured to provide the number for records to write to a local arrow file when storing.
As such the above failure state could occur during either the writing or the reading of the file.

Writing of the local file is performed with ArrowRecordBatch.sortArrowAndWriteToLocalFile, and as such one record batch at a time
is held in smallBatchVectorSchemaRoot. This also needs to fit alongside the vector of indexes for the data.
Meaning that it is possible for one of the following places to throw an exception for the fail state:

    - smallBatchVectorSchemaRoot.allocateNew
    - dstVector.copyFromSafe
The execption thrown is likely to be an OutOfMemoryException.

The reading of the batch file occurs within ArrowRecordBatch.createCloseableRecordIteratorForArrowFile. As part of this 
process the ArrowStreamReader handles the allocation of the file and will hold one record batch within its ArrowReader.
As it will also require memory for the MessageChannelReader during the process, all of this is likely to contribute to 
the failure state