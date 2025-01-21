Common problems and their solutions
===================================

These instructions will assume you start in the project root directory and Sleeper has been built
(see [the developer guide](developer-guide.md) for how to set that up).

## Out of memory error from standard ingest tasks

Presently the implementation is based on Arrow. Previously it used an array list, and will work differently if that is
used. Also see documentation on the [record batch types](design/ingest-record-batch-types.md).


### Arrow implementation

#### One record doesn't fit into the whole batch buffer

This would occur if a single record is very large. An error is thrown when the record is written. This can be resolved
by expanding the batch buffer size. Alternatively, it may be possible to break the records into more manageable chunks.

#### Vector of indexes doesn't fit in the working buffer

This can occur when the records are small enough that when the batch buffer is full, a vector of indexes would be too
long to fit in the working buffer. We need one index for each record to store its position in the sort order. For
example, if each record was a single integer, the vector of indexes would be a similar length to the batch buffer.

If this doesn't fit on its own, an OutOfMemoryException will be thrown when the vector of indexes is allocated. This can
be resolved by expanding the working buffer size and/or reducing the batch buffer size.

If it does fit, it can fill the working buffer enough that an Arrow record batch does not fit, causing errors described
in the section below.

#### Arrow record batch doesn't fit in the working buffer

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


### Array list implementation

If standard ingest tasks fail with an out of memory error ("Exception in thread main java.lang.OutOfMemoryError: Java
heap space") then this is likely due to the tasks not being able to store the specified number of records in memory.
Standard ingest works by reading a certain number of records (given by `sleeper.ingest.memory.max.batch.size`) into
memory. These are sorted and then written to a local file. This process is repeated a certain number of times until a
certain number of records in total (given by `sleeper.ingest.max.local.records`) have been written to local disk.
Sensible values for the parameters `sleeper.ingest.memory.max.batch.size` and `sleeper.ingest.max.local.records`
obviously depend on the data - the more fields the schema has and the bigger those fields are, the more space will be
used and the fewer records will fit into memory / on disk.

If you see an out of memory error, then try reducing `sleeper.ingest.memory.max.batch.size`. When reducing this
parameter it is a good idea to also reduce `sleeper.ingest.max.local.records`. To change these parameters, use the
administration client described in the [deployment guide](deployment-guide.md#sleeper-administration-client).


## I created an instance, destroyed it and then recreating it failed

If you create an instance and destroy it then some remnants of the previous instance will still be present. Usually this
should be log groups containing logs of the previous instance.

The CDK deployment process can also be configured to not delete the buckets for the tables, or the bucket for the
results of queries. This is set in the `sleeper.retain.infra.after.destroy` instance property. It may also be because
the `cdk destroy` command partially failed due to there being some tasks running on ECS or EMR clusters. In this case
the cluster cannot be destroyed until the tasks are completed or terminated.

If there are some remnants present, then attempting to deploy Sleeper again with the same instance id will fail as it
will complain that some resources it needs to create already exist.

If you want to recreate an instance with the same instance id as one that was previously deleted, then check
that all resources with a name containing that instance id have been deleted.
