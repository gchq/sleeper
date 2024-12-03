Common problems and their solutions
===================================

These instructions will assume you start in the project root directory and Sleeper has been built
(see [the developer guide](12-dev-guide.md) for how to set that up).

## Out of memory error from standard ingest tasks

If standard ingest tasks fail with an out of memory error ("Exception in thread main java.lang.OutOfMemoryError: Java
heap space") then this is likely due to the tasks not being able to store the specified number of records in memory.
Standard ingest works by reading a certain number of records (given by `sleeper.ingest.arrow.working.buffer.bytes`) into
memory. These are sorted and then written to a local file. This process is repeated a certain number of times until a
certain number of records in total (given by `sleeper.ingest.arrow.max.single.write.to.file.records`) have been written to local disk.
Sensible values for the parameters `sleeper.ingest.arrow.max.local.store.bytes` and `sleeper.ingest.arrow.batch.buffer.bytes`
obviously depend on the data - the more fields the schema has and the bigger those fields are, the more space will be
used and the fewer records will fit into memory / on disk.

If you see an out of memory error, then try reducing `sleeper.ingest.arrow.max.local.store.bytes`. When reducing this
parameter it is a good idea to also reduce `sleeper.ingest.arrow.batch.buffer.bytes`. To change these parameters, use the
administration client described in the [system status documentation](06-status.md).

Presently the implemenation is based on arrow ingest, whereas previously it was an arraylist based implementation and
as such will work differently if that is used.

## I created an instance, destroyed it and then recreating it failed

If you create an instance and destroy it then some remnants of the previous instance will still be present. Usually this
should be log groups containing logs of the previous instance.

The CDK deployment process can also be configured to not delete the buckets for the tables, or the bucket for the
results of queries. This is set in the `sleeper.retain.infra.after.destroy` instance property. It may also be because
the `cdk destroy` command partially failed due to there being some tasks running on ECS or EMR clusters. In this case
the cluster cannot be destroyed until the tasks are completed or terminated.

If there are some remnants present then attempting to deploy Sleeper again with the same instance id will fail as it
will complain that some resources it needs to create already exist.

If you want to recreate an instance with the same instance id as one that was previously deleted, then check
that all resources with a name containing that instance id have been deleted.
