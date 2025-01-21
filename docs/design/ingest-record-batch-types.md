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
