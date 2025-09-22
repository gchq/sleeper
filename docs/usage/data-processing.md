Data Processing
===============

Sleeper supports three modes of data processing that can be applied either to a table as a whole, or to an individual
query:

- Aggregation of rows with the same row key and sort key values
- Filtering of rows
- Custom iterators over sorted data

Aggregation and filtering can be configured against a Sleeper table in table properties, and will be applied whenever
the table data is read. These are implemented in both the Java and DataFusion data engines. This means that if the table
is configured to run compaction in DataFusion, but queries run in Java, the same logic will be applied in both places.
In time we will replace the Java implementation with queries in DataFusion as well.

Custom iterators let you add your own data processing. This can be configured either against a Sleeper table in table
properties, or in a field on a query. This is currently only possible with the Java data engine.

Note that you can choose which data engine should be used in the table property
[`sleeper.table.data.engine`](properties/table/data_definition.md).

Aggregation and filtering configuration should be preferred in all cases over custom iterators, as it is much more
efficient to apply this in DataFusion than to apply iterators in Java. If you set a custom iterator against a table,
this forces use of the Java data engine for compaction. Compaction in Java is much slower and more expensive, so this is
not recommended.

We intend to keep the configuration as simple as possible for aggregation and filtering, rather than introducing many
operations and options. We are planning alternatives to apply other types of processing in DataFusion.

## Types of processing

### Aggregation

There are many use cases where we want to aggregate rows where the keys are the same. We can configure this in the table
property [`sleeper.table.aggregations`](properties/table/data_definition.md).

For example, we have a three column table where the key field is a string called 'id', the first value field is a long
called 'count' and the second value field is a long called 'last_seen'. If multiple rows with the same id are inserted
then we want to add the counts and take the maximum of the values in the last_seen column. We can achieve this by
setting `sleeper.table.aggregations` to `sum(count), max(last_seen)`.

This means that every time a compaction or a query runs, rows will be combined together if they have the same value for
all row keys and sort keys. This becomes part of the definition of the table, and you will only ever see the data with
that process pre-applied.

See the the [table properties documentation](properties/table/data_definition.md) for the supported aggregation
operations. We also support this for value fields that contain nested map data. This can be applied with the Java or
DataFusion data engine.

### Filtering

There are use cases that require filtering of data based on values of fields. We can configure this in the table
property [`sleeper.table.filters`](properties/table/data_definition.md).

For example, if the rows in our table have a long field called 'timestamp', we may want to remove a row whenever the
timestamp is older than 2 weeks. We can achieve this by setting `sleeper.table.filters`
to `ageOff(timestamp, 1209600000)`. The second parameter is the maximum age in milliseconds.

This means that every time a compaction or a query runs, rows will be excluded from the output if they have a timestamp
that's older than the specified age. As this is applied during compactions, the data which is too old is effectively
deleted from the table. This becomes part of the definition of the table, and you will never see data that has a
timestamp older than that according to the clock of the query processor.

See the the [table properties documentation](properties/table/data_definition.md) for the supported filtering
operations. This can be applied with the Java or DataFusion data engine.

### Custom iterators

For arbitrary data processing, you can write your own iterator implementing the Java interface `ConfigStringIterator`.
This lets you insert operations to be performed on rows as Sleeper reads the underlying data, which is usually done in
parallel across many machines.

You can apply a custom iterator against a query. This is much cheaper to apply than during compaction as it processes
much less data, and does not force use of a particular data engine. This will not affect the underlying data in the
table. A good use case for this is for filtering results based on a user's permissions.

You can apply a custom iterator against a Sleeper table to apply it both during compactions and during queries. This
will apply the results to the table as a whole persistently. That forces use of the Java data engine, which is much more
expensive and slower. As a result we do not recommend this.

Note that any custom iterator will have an impact on startup time during queries, as your code will be loaded from S3 at
runtime.

A custom iterator is a function that takes as input a `CloseableIterator<Row>` and returns a `CloseableIterator<Row>`.
Examples of iterators that perform aggregation or filtering can be found in the `example-iterators` module. The iterator
should respect the general constraints of a compaction: there could be many hundreds of millions of rows processed by a
single compaction job, so there should be no attempt to buffer lots of rows in memory; there is no guarantee of the
order the files in a partition will be compacted, or that all of them will be compacted at the same time so the logic
should be commutative and associative; the output should be sorted by key so in general the row and sort keys should not
be changed by the iterator.

If one of the fields in a table is a byte array then that could be a serialised version of an arbitrary Java object.
This allows aggregation of fields that contain complex values, e.g. Accumulo's iterators in
[Gaffer](https://github.com/gchq/Gaffer) are used to maintain HyperLogLog sketches which are used to quickly
approximate the degree of a vertex.

#### Configuration

To include your own jar in the classpath to retrieve this iterator, upload it to the jars bucket configured in the
instance property `sleeper.jars.bucket`, and add the object key to a comma-separated list in the instance property
`sleeper.userjars`. See the [instance properties documentation](properties/instance/user/common.md).

This will usually be used in a query. In Java you can set this in the `QueryProcessingConfig` object that's set in
the field `Query.processingConfig`. When submitting a query as JSON, you can set the JSON
fields `queryTimeIteratorClassName` and `queryTimeIteratorConfig`. The iterator class name field should be the fully
qualified name of your class that implements `ConfigStringIterator`. The iterator config should be the string you want
to be passed into `ConfigStringIterator.init`.

To apply a custom iterator to write the results back to the table, during both compactions and queries, you can set it
in the table properties. We may remove this in the future due to the performance and cost implications. This is set in
the table properties `sleeper.table.iterator.class.name` and `sleeper.table.iterator.config`. See
the [table properties documentation](properties/table/data_definition.md).
