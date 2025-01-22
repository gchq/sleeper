Creating a schema
==================

A schema describes the data in a table. A schema consists of several fields. A field has a
name and a type, e.g. a field with name 'id' and type 'string'. A schema has three types of field:
row keys, sort keys and values. 

See [a full example](../../example/full/schema.json) for an example of a schema. 

For example, a simple key-value schema with a string key and a string value would allow records to be
retrieved by querying for a key. The schema for this would be:

```JSON
    {
      "rowKeyFields": [
        {
          "name": "key",
          "type": "StringType"
        }
      ],
      "sortKeyFields": [],
      "valueFields": [
        {
          "name": "value",
          "type": "StringType"
        }
      ]
    }
```

Note that if there are no sort or value fields then this must be indicated with an empty list.

If we wanted to sort the records for a particular field by a timestamp, we could add a sort field of type long:

```JSON
    {
      "rowKeyFields": [ 
        {
          "name": "key",
          "type": "StringType"
        }
      ],
      "sortKeyFields": [
        {
          "name": "timestamp",
          "type": "LongType"
        }
      ],
      "valueFields": [
        {
          "name": "value",
          "type": "StringType"
        }
      ]
    }
```

This would cause records for a particular key to be stored (and retrieved) in increasing order of timestamps.

The following types are permitted as row keys and sort keys: `IntType`, `LongType`, `StringType`, `ByteArrayType`. All
of these types can be used for values. Additionally, value fields may be of type `ListType` or `MapType`. Here is an
example schema where there are several value fields:

```JSON
    {
      "rowKeyFields": [
        {
          "name": "key",
          "type": "StringType"
        }
      ],
      "sortKeyFields": [
        {
          "name": "timestamp",
          "type": "LongType"
        }
      ],
      "valueFields": [
        {
          "name": "value1",
          "type": "StringType"
        },
        {
          "name": "value2",
          "type": "ByteArrayType"
        },
        {
          "name": "value3",
          "type": {
            "ListType": {
              "elementType": "IntType"
            }
          }
        },
        {
          "name": "value4",
          "type": {
            "MapType": {
              "keyType": "IntType",
              "valueType": "StringType"
            }
          }
        }
      ]
    }
```

The field with name `value3` is a list with integer elements. The field with name `value4` is a map with integer keys and string values.

There may be multiple row key fields. In the following example two string fields are used as row keys:

```JSON
    {
      "rowKeyFields": [ 
        {
          "name": "key1",
          "type": "StringType"
        },
        {
          "name": "key2",
          "type": "StringType"
        }
      ],
      "sortKeyFields": [
        {
          "name": "timestamp",
          "type": "LongType"
        }
      ],
      "valueFields": [
        {
          "name": "value",
          "type": "StringType"
        }
      ]
    }
```

Sleeper will store the records sorted by `key1` and then `key2`. Thus retrieving all records where `key1`
and `key2` have specified values will be quick. A range scan to retrieve all records where `key1` has a certain
value and `key2` can take any value will also be quick. But a query for all records where `key2` has a specified
value but `key1` can take any value will not be quick.
