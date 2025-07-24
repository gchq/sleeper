Ingest record batch types
=========================

Standard ingest involves caching records in a record batch as they come in. The record batch is responsible for holding
records, sorting them, and then returning them back in order to be written to the Sleeper table. In practice a record
batch holds some records in memory, then when the memory fills up it sorts the records and writes them to the local
disk. When the store on the local disk fills up it reports that it's full.

When the record batch is full or the ingest finishes, the records are written to the Sleeper table as a minor compaction
where all the files in the local disk are merged together in sort order.

We have multiple implementations of a record batch. Currently the default implementation uses Arrow, and we also have a
version backed by a Java ArrayList. Both are described below.

Also see information on possible out of memory errors
in [common problems and their solutions](../common-problems-and-their-solutions.md#out-of-memory-error-from-standard-ingest-tasks).

## Arrow implementation

The Arrow record batch implementation uses two Arrow buffers, called the batch buffer and the working buffer.

The batch buffer holds Sleeper records as they come in, until it fills up and they are written to a local Arrow file.
The working buffer holds the sort order when sorting the records, and a page of records at a time when writing to and
reading from local files.

The sort order is a vector of indexes that each point to a record at that position in the order. This is updated
during sorting so that the record data is unchanged.

When reading from or writing to a local file, one Arrow record batch is held at a time, containing a page of records.
Each record batch is created just before it's written to a local file by copying from the batch buffer to the working
buffer following the sort order in the vector of indexes.

When reading a local file the working buffer is also used for some extra metadata used by Arrow. This can usually be
ignored as the sort order is not held in memory at this point.

### Size requirements and configuration

The working buffer needs to be big enough to hold the sort order and one record batch at the same time. The sort order
vector will hold one integer for every record in the batch buffer. The size of the batch buffer and the working buffer
are configurable. The number of records in a record batch is also configurable, which will take up some amount of space
in the working buffer.

Note that the amount of space needed for a record is not constant, as nested data may be held that can vary per record.

The relationship between the size of a record and the size it requires in the sort order vector will vary. If a record
is large the batch buffer can be much larger than the working buffer. If a record is very small the working buffer will
need to be closer in size to the batch buffer, and the sort order will take up proportionally more of the space in the
working buffer.

The maximum number of records to hold in the batch buffer at once is set in the instance
property `sleeper.ingest.arrow.max.single.write.to.file.records`. The size of the batch buffer is set in the instance
property `sleeper.ingest.arrow.batch.buffer.bytes`. The size of the local store is set in the instance
property `sleeper.ingest.arrow.max.local.store.bytes`. The working buffer size is set in the instance
property `sleeper.ingest.arrow.working.buffer.bytes`.

This requires enough memory on an ingest task to hold both the working buffer and the batch buffer. The number of
records this can accept is partially flexible. The record batch will be flushed to disk when the batch buffer fills up,
rather than based on the number of records.

## Array list implementation

The array list record batch implementation holds records in a Java ArrayList, until it fills up and they are sorted and
written to a local Parquet file. The Parquet library is responsible for holding intermediate data when writing to and
reading from local files. The size of the array and the local store are both configured in terms of the number of
records. The amount of space this takes up will vary depending on the size of the records.

The maximum number of records to hold in the ArrayList at once is set in the instance
property `sleeper.ingest.memory.max.batch.size`. The maximum number of records to hold in local files at once is set in
the instance property `sleeper.ingest.max.local.records`.

This requires enough memory on an ingest task to hold the given number of records in an ArrayList.
