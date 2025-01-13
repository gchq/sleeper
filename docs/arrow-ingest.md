## Arrow ingest record batch type

The Arrow ingest design uses a batch buffer to hold Sleeper records as they come in. If this buffer fills up, when the
next record is added the whole batch is sorted and written to a local file. When this local store fills up, or when the
ingest finishes, the local files are merged together as a minor compaction and written to the Sleeper table.

The size of the batch buffer is set in the instance property `sleeper.ingest.arrow.batch.buffer.bytes`. The size of the
local store is set in the instance property `sleeper.ingest.arrow.max.local.store.bytes`.

A working buffer is also needed when sorting and writing to a local file, and when reading the local files into the
Sleeper table. The working buffer size is set in the instance property `sleeper.ingest.arrow.working.buffer.bytes`.

### Working buffer

The working buffer holds a vector of indexes to establish a sort order of the records. One integer is held for each
record. This is updated during sorting, so that the record data remains unchanged.

The working buffer holds one Arrow record batch at a time when writing a local file. These are like pages of the file.
Each is created by copying from the batch buffer to the working buffer following the sort order in the vector of
indexes. The size of an Arrow record batch is set in the instance
property `sleeper.ingest.arrow.max.single.write.to.file.records`.

When reading a local file the working buffer is also used for some extra metadata used by Arrow.

## Possible failure states:

### One record doesn't fit into the whole batch buffer

This would occur if a single record is very large. An error is thrown when the record is written. This can be resolved
by expanding the batch buffer size. Alternatively, it may be possible to break the records into more manageable chunks.

### Vector of indexes doesn't fit in the working buffer

This can occur when the records are small enough that when the batch buffer is full, a vector of indexes would be too
long to fit in the working buffer. We need one index for each record to store its position in the sort order. For
example, if each record was a single integer, the vector of indexes would be a similar length to the batch buffer.

If this doesn't fit on its own, an OutOfMemoryException will be thrown when the vector of indexes is allocated. This can
be resolved by expanding the working buffer size and/or reducing the batch buffer size.

If it does fit, it can fill the working buffer enough that an Arrow record batch does not fit, causing errors described
in the section below.

### Arrow record batch doesn't fit in the working buffer

The size of an Arrow record batch is configured as a number of records, but this needs to fit in the working buffer,
which is configured as a number of bytes. If these don't relate as expected, this could cause a failure at any time
memory is allocated on the working buffer, either when writing or reading a local file. In both cases we hold one Arrow
record batch at a time in the working buffer.

When writing a local file, the Arrow record batch needs to fit alongside the vector of indexes for the sort order. An
exception can be thrown either when we allocate the record batch, or when we copy record data into the record batch. The
exception thrown is likely to be an OutOfMemoryException.

When reading a local file, Arrow requires some extra memory for the file's metadata. This is also held in the working
buffer. Any exception would come from Arrow. This is unlikely as a file would need to have been written first.

This can be resolved by increasing the size of the working buffer, or reducing the number of records to write to a local
file at once. It's also possible that this failure can be caused by a large vector of indexes, which could be resolved
by reducing the batch buffer size, also see the section above.
