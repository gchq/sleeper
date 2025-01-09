## Arrow ingest record batch type

The Arrow ingest design uses a batch buffer. This buffer is used for holding the records of a local file. The Sleeper 
records are then written to this buffer.

If this buffer fills up, when the next record is added the whole batch is then sorted. 
Once complete the full data is written to a local file. See the below section for further information.

### Working buffer

The working buffer is used for a vector of indexes when the process is sorting the records. 1 integer is provided for 
each record. It is this buffer that is updated during sorting, meaning that the record data will remain unchanged.
When writing the local file, it is constructed by using arrow record batches, these are akin to pages within the file.
The batches are held in memory one at a time when attempting to write them to the file.
A record batch is created by copying from the batch buffer to the working buffer following the sort order present within
the vector of indexes. The working buffer memory is shared between the record batch and the sort indexes.

An ingest will gather files until the store is filled or until all the remaining records are in the local store.
The size of the store is defined by the property: 'sleeper.ingest.arrow.max.local.store.bytes'.
Once done, it will merge them all together and will pass the sorted records through to the Sleeper table. This opens all
the files and reads one record batch for each file. Then it will hold one record batch for each file within memory at a 
time as it scans through the data in all the files.

## Possible failure states:

### One record doesn't fit into the whole batch buffer

This would occur if the record itself is very large in size (batch buffer size is defined by `sleeper.ingest.arrow.batch.buffer.bytes`)
An AssertionError is thrown during the execution of flushing the fille to local. [ArrowRecordBatch.flushToLocalArrowFileThenClear]
Resolution for this would primarily be through expanding the buffer size if possible, alternatively adjust the input to 
break into more manageable chunks should allow avoidance of this issue.

### Vector of indexes doesn't fit in the working buffer
This is likely to occur when the records are small enough that when the batch buffer is full we would have too many integers 
to fit in the working buffer as a vector of the indexes. For example, if the record was a single integer, the vector of the 
indexes would likely be similar in length to that of the batch buffer. An OutOfMemoryException would be thrown at the 
allocation of the vector of indexes. The likely resolution for this is to expand the working buffer capacity.

### Arrow record batch doesn't fit in the working buffer
The batch size of the records is configured to provide the number for records to write to a local arrow file when storing.
As such this failure state could occur during either the writing or the reading of the file.

When writing to the local file, we hold one Arrow record batch at a time in the working buffer. This also needs to fit 
alongside the vector of indexes for the data. Thre exception can be thrown either when we allocate the record batch or
when we copy record data into the record batch. The exception thrown is likely to be an OutOfMemoryException.

The reading of the batch file occurs within ArrowRecordBatch.createCloseableRecordIteratorForArrowFile. As part of this 
process the ArrowStreamReader handles the allocation of the file and will hold one record batch within its ArrowReader.
As it will also require memory for the MessageChannelReader during the process, all of this is likely to contribute to 
the failure state.

## To explain
sleeper.ingest.arrow.working.buffer.bytes
sleeper.ingest.arrow.batch.buffer.bytes
sleeper.ingest.arrow.max.local.store.bytes
sleeper.ingest.arrow.max.single.write.to.file.records