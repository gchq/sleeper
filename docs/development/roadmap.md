Roadmap
=======

This section documents some of the improvements that we plan to make to Sleeper.

The following improvements are actively being worked on:

- https://github.com/gchq/sleeper/issues/3446 Bulk export.
- https://github.com/gchq/sleeper/issues/1330 Support deploying a published version of Sleeper.
- https://github.com/gchq/sleeper/issues/4155 Java API improvements.
- https://github.com/gchq/sleeper/issues/3687 Update design documentation.
- https://github.com/gchq/sleeper/issues/4401 Garbage collector batching.

The following are likely to be worked on in the near future:

- https://github.com/gchq/sleeper/issues/4393 Batch up partition splitting commits.
- https://github.com/gchq/sleeper/issues/3693 Declarative deployment for infrastructure as code.
- https://github.com/gchq/sleeper/issues/1389 Upgrade to AWS SDK v2.
- https://github.com/gchq/sleeper/issues/1391 Create a library of repeatable, sustained, large-scale performance tests.
- https://github.com/gchq/sleeper/issues/1388 Rust implementations for operations on data files.

The following improvements will be worked on in future (these are in no particular order):

- https://github.com/gchq/sleeper/issues/576 Use Arrow types in the table schema.
- https://github.com/gchq/sleeper/issues/4396 Failure handling / backpressure for state store updates.
- https://github.com/gchq/sleeper/issues/4398 Trigger compaction dispatch in transaction log follower.
- Scaling improvements.
    - https://github.com/gchq/sleeper/issues/4215 Service that maintains an up-to-date cache of the state store.
    - https://github.com/gchq/sleeper/issues/4218 Batch up updates to job trackers from state store commits.
    - https://github.com/gchq/sleeper/issues/4525 Mitigate transaction throughput limitations.
    - https://github.com/gchq/sleeper/issues/4214 Mitigate memory limitations with multiple Sleeper tables.
    - https://github.com/gchq/sleeper/issues/4395 Table state partitioning.
    - https://github.com/gchq/sleeper/issues/4394 Parallelise garbage collection.
- Usability improvements.
    - https://github.com/gchq/sleeper/issues/1328 Unify admin client and related scripts.
    - https://github.com/gchq/sleeper/issues/1786 REST API.
    - Python API improvements. This is currently basic and needs further work.
- https://github.com/gchq/sleeper/issues/1392 Create a predicate language for specifying filters on queries.
- https://github.com/gchq/sleeper/issues/1390 Review and extend the integrations with Athena and Trino.
- Metrics page. Review and extend the metrics produced.
- Purge data from a table, i.e. delete any items matching a predicate.
