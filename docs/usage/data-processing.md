Data Processing
===============

Sleeper supports three modes of data processing that can be applied either to a table as a whole, or to an individual
query:

- Aggregation of rows with the same row key and sort key
- Filtering of rows
- Custom iterators over sorted data

### Aggregation

There are many use cases where we want to aggregate rows where the keys are the same, e.g. we have a three column
table where the key field is a string called 'id', the first value field is a long called 'count' and the second
value field is a long called 'last_seen'. If multiple rows with the same id are inserted then we want to add the
counts and take the maximum of the values in the last_seen column.

### Filtering

There are use cases that require filtering of data based on values of fields, e.g. if the rows in our table have a
timestamp field we may want to remove a row whenever the timestamp is more than a certain age.
