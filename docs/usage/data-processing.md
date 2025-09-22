Data Processing
===============

Sleeper supports three modes of data processing that can be applied either to a table as a whole, or to an individual
query:

- Aggregation of rows with the same row key and sort key values
- Filtering of rows
- Custom iterators over sorted data

Aggregation and filtering can be configured against a Sleeper table in table properties, and will be applied whenever
the table data is read. These are implemented in both the Java and DataFusion data engines.

Custom iterators can be configured either against a Sleeper table in table properties, or in a field on a query. This is
currently only supported in Java. If you set a custom iterator against a table, this forces compactions to use the Java
data engine. Compaction in Java is much slower and more expensive, so this is not recommended.

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
that's older than the specified age. This becomes part of the definition of the table, and you will never see data that
has a timestamp older than that according to the clock of the query processor.

See the the [table properties documentation](properties/table/data_definition.md) for the supported filtering
operations. This can be applied with the Java or DataFusion data engine.

### Custom iterators

For arbitrary data processing, you can write your own iterator implementing the Java interface `ConfigStringIterator`.
This lets you insert operations to be performed on rows as Sleeper reads the underlying data, which is usually done in
parallel across many machines. Note that this will have an impact on startup time during queries, as your code will be
loaded from S3 at runtime.

To include your own jar in the classpath to retrieve this iterator, upload it to the jars bucket configured in the
instance property `sleeper.jars.bucket`, and add the object key to a comma-separated list in the instance property
`sleeper.userjars`. See the [instance properties documentation](properties/instance/user/common.md).

This should usually be used in a query. In Java you can set this in the `QueryProcessingConfig` object that's set in
the field `Query.processingConfig`. When submitting a query as JSON, you can set the JSON
fields `queryTimeIteratorClassName` and `queryTimeIteratorConfig`. The iterator class name field should be the fully
qualified name of your class that implements `ConfigStringIterator`. The iterator config should be the string you want
to be passed into `ConfigStringIterator.init`.

We currently allow setting this to be applied during compactions as well, as part of the definition of a table. This
forces use of the Java data engine for compaction. Compaction in Java is much slower and more expensive, so this is not
recommended. We may remove this in the future. This is set in the table properties `sleeper.table.iterator.class.name`
and `sleeper.table.iterator.config`. See the the [table properties documentation](properties/table/data_definition.md).
