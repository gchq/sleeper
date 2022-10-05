The files in this directory contain split points that can be used to pre-split the tables used in the
standard system test. The standard system test uses a schema where the row key is a string. The data
that is randomly generated consists of row keys which are strings of length 10 with characters from
the alphabet a-z.

For example the file 8-partitions.txt contains 7 split points that can be used to split the key space
into 8 approximately evenly sized partitions.
