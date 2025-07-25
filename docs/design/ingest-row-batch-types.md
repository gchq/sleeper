Ingest row batch types
=========================

Standard ingest involves caching rows in a row batch as they come in. The row batch is responsible for holding
rows, sorting them, and then returning them back in order to be written to the Sleeper table. In practice a row
batch holds some rows in memory, then when the memory fills up it sorts the rows and writes them to the local
disk. When the store on the local disk fills up it reports that it's full.

When the row batch is full or the ingest finishes, the rows are written to the Sleeper table as a minor compaction
where all the files in the local disk are merged together in sort order.

We have multiple implementations of a row batch. Currently the default implementation uses Arrow, and we also have a
version backed by a Java ArrayList. Both are described below.

Also see information on possible out of memory errors
in [common problems and their solutions](../common-problems-and-their-solutions.md#out-of-memory-error-from-standard-ingest-tasks).

## Arrow implementation

The Arrow row batch implementation uses two Arrow buffers, called the batch buffer and the working buffer.

The batch buffer holds Sleeper rows as they come in, until it fills up and they are written to a local Arrow file.
The working buffer holds the sort order when sorting the rows, and a page of rows at a time when writing to and
reading from local files.

The sort order is a vector of indexes that each point to a row at that position in the order. This is updated
during sorting so that the row data is unchanged.

When reading from or writing to a local file, one Arrow row batch is held at a time, containing a page of rows.
Each row batch is created just before it's written to a local file by copying from the batch buffer to the working
buffer following the sort order in the vector of indexes.

When reading a local file the working buffer is also used for some extra metadata used by Arrow. This can usually be
ignored as the sort order is not held in memory at this point.

### Size requirements and configuration

The working buffer needs to be big enough to hold the sort order and one row batch at the same time. The sort order
vector will hold one integer for every row in the batch buffer. The size of the batch buffer and the working buffer
are configurable. The number of rows in a row batch is also configurable, which will take up some amount of space
in the working buffer.

Note that the amount of space needed for a row is not constant, as nested data may be held that can vary per row.

The relationship between the size of a row and the size it requires in the sort order vector will vary. If a row
is large the batch buffer can be much larger than the working buffer. If a row is very small the working buffer will
need to be closer in size to the batch buffer, and the sort order will take up proportionally more of the space in the
working buffer.

The maximum number of rows to hold in the batch buffer at once is set in the instance
property `sleeper.ingest.arrow.max.single.write.to.file.rows`. The size of the batch buffer is set in the instance
property `sleeper.ingest.arrow.batch.buffer.bytes`. The size of the local store is set in the instance
property `sleeper.ingest.arrow.max.local.store.bytes`. The working buffer size is set in the instance
property `sleeper.ingest.arrow.working.buffer.bytes`.

This requires enough memory on an ingest task to hold both the working buffer and the batch buffer. The number of
rows this can accept is partially flexible. The row batch will be flushed to disk when the batch buffer fills up,
rather than based on the number of rows.

## Array list implementation

The array list row batch implementation holds rows in a Java ArrayList, until it fills up and they are sorted and
written to a local Parquet file. The Parquet library is responsible for holding intermediate data when writing to and
reading from local files. The size of the array and the local store are both configured in terms of the number of
rows. The amount of space this takes up will vary depending on the size of the rows.

The maximum number of rows to hold in the ArrayList at once is set in the instance
property `sleeper.ingest.memory.max.batch.size`. The maximum number of rows to hold in local files at once is set in
the instance property `sleeper.ingest.max.local.rows`.

This requires enough memory on an ingest task to hold the given number of rows in an ArrayList.
