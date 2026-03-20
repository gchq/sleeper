Roadmap
=======

This section documents some of the improvements that we plan to make to Sleeper.

The following improvements are actively being worked on:

- https://github.com/gchq/sleeper/issues/5781 Easier CDK-focused deployment.

The following are likely to be worked on in the near future:

- https://github.com/gchq/sleeper/issues/5078 Improvements to bulk import.
- https://github.com/gchq/sleeper/issues/6132 Create a Spark DataFrame from a Sleeper table.
- https://github.com/gchq/sleeper/issues/4215 Optional long running service for queries.
- https://github.com/gchq/sleeper/issues/4235 Graceful upgrade of a Sleeper instance.

The following improvements will be worked on in future (these are in no particular order):

- https://github.com/gchq/sleeper/issues/6059 Data types for floating point value fields.
- https://github.com/gchq/sleeper/issues/6058 Notifications for ingest progress.
- https://github.com/gchq/sleeper/issues/6117 Instance health checks.
- https://github.com/gchq/sleeper/issues/4213 Batch up partition splitting commits.
- https://github.com/gchq/sleeper/issues/1391 Create a library of repeatable, sustained, large-scale performance tests.
- https://github.com/gchq/sleeper/issues/1393 Bulk export queries, tracking, restore from export.
- https://github.com/gchq/sleeper/issues/4396 Failure handling / backpressure for state store updates.
- https://github.com/gchq/sleeper/issues/3693 Improvements to declarative deployment with infrastructure as code.
- https://github.com/gchq/sleeper/issues/576 Use Arrow types in the table schema.
- https://github.com/gchq/sleeper/issues/4398 Trigger compaction dispatch in transaction log follower.
- Scaling improvements.
    - https://github.com/gchq/sleeper/issues/4525 Mitigate limitations on throughput of state store updates.
    - https://github.com/gchq/sleeper/issues/4218 Batch up updates to job trackers from state store commits.
    - https://github.com/gchq/sleeper/issues/5674 Table state compression.
    - https://github.com/gchq/sleeper/issues/4555 Optimise table snapshot creation.
- Usability improvements.
    - https://github.com/gchq/sleeper/issues/1328 Unify admin client and related scripts.
    - https://github.com/gchq/sleeper/issues/1786 REST API.
    - Python API improvements. This is currently basic and needs further work.
- https://github.com/gchq/sleeper/issues/1392 Create a predicate language for specifying filters on queries.
- https://github.com/gchq/sleeper/issues/1390 Review and extend the integrations with Athena and Trino.
- https://github.com/gchq/sleeper/issues/5675 Visibility of long term tracker metrics.
- Metrics page. Review and extend the metrics produced.
- Purge data from a table, i.e. delete any items matching a predicate.

We also have an article on [potential deployment improvements](deployment-improvements.md), examining how the current
deployment setup relates to the planned improvements linked above.
