Data Processing
===============

Sleeper supports three modes of data processing that can be applied either to a table as a whole, or to an individual
query:

- Aggregation of rows with the same row key and sort key
- Filtering of rows
- Custom iterators over sorted data

Each of these have a table property and a field on a query where they can be set to be applied.

## Types of processing

### Aggregation

There are many use cases where we want to aggregate rows where the keys are the same, e.g. we have a three column
table where the key field is a string called 'id', the first value field is a long called 'count' and the second
value field is a long called 'last_seen'. If multiple rows with the same id are inserted then we want to add the
counts and take the maximum of the values in the last_seen column.

This can be set in the table property [`sleeper.table.aggregations`](properties/table/data_definition.md), or in the
field ... on a query passed as JSON.

### Filtering

There are use cases that require filtering of data based on values of fields, e.g. if the rows in our table have a
timestamp field we may want to remove a row whenever the timestamp is more than a certain age.

### Custom iterators

TODO

## Table data processing

TODO

## Query data processing

TODO
