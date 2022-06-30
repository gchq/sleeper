package sleeper.ingest.testutils;

import org.apache.commons.text.RandomStringGenerator;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.*;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RecordGenerator {
    public static <T> RecordListAndSchema genericKey1D(Type sleeperKeyTypeDimension0,
                                                       List<T> keyObjectsDimension0) {
        int noOfRecords = keyObjectsDimension0.size();
        Random valueRandom = new Random(0);
        RandomStringGenerator randomStringGenerator = new RandomStringGenerator.Builder()
                .usingRandom(valueRandom::nextInt)
                .build();
        Schema sleeperSchema = new Schema();
        sleeperSchema.setRowKeyFields(
                new Field("key0", sleeperKeyTypeDimension0));
        sleeperSchema.setValueFields(
                new Field("intValue", new IntType()),
                new Field("longValue", new LongType()),
                new Field("stringValue", new StringType()),
                new Field("byteArrayValue", new ByteArrayType()),
                new Field("listOfStringsValue", new ListType(new StringType())),
                new Field("mapFromLongToStringValue", new MapType(new LongType(), new StringType())));
        List<Record> recordList = IntStream.range(0, noOfRecords)
                .mapToObj(i -> {
                    Record record = new Record();
                    record.put(sleeperSchema.getRowKeyFieldNames().get(0), keyObjectsDimension0.get(i));
                    record.put(sleeperSchema.getValueFieldNames().get(0), valueRandom.nextInt());
                    record.put(sleeperSchema.getValueFieldNames().get(1), valueRandom.nextLong());
                    record.put(sleeperSchema.getValueFieldNames().get(2), randomStringGenerator.generate(valueRandom.nextInt(50)));
                    record.put(sleeperSchema.getValueFieldNames().get(3), randomStringGenerator.generate(valueRandom.nextInt(50)).getBytes(StandardCharsets.UTF_8));
                    record.put(
                            sleeperSchema.getValueFieldNames().get(4),
                            IntStream.range(0, valueRandom.nextInt(10))
                                    .mapToObj(dummy -> randomStringGenerator.generate(valueRandom.nextInt(50)))
                                    .collect(Collectors.toList()));
                    record.put(
                            sleeperSchema.getValueFieldNames().get(5),
                            IntStream.range(0, valueRandom.nextInt(10)).boxed()
                                    .collect(Collectors.toMap(
                                            dummy -> valueRandom.nextLong(),
                                            dummy -> randomStringGenerator.generate(valueRandom.nextInt(50)))));
                    return record;
                }).collect(Collectors.toList());
        Collections.shuffle(recordList, new Random(0));
        return new RecordListAndSchema(recordList, sleeperSchema);
    }

    public static <T, U> RecordListAndSchema genericKey2D(Type sleeperKeyTypeDimension0,
                                                          Type sleeperKeyTypeDimension1,
                                                          List<T> keyObjectsDimension0,
                                                          List<U> keyObjectsDimension1) {
        int noOfRecords = keyObjectsDimension0.size();
        if (keyObjectsDimension1.size() != noOfRecords) {
            throw new AssertionError();
        }
        Random valueRandom = new Random(0);
        RandomStringGenerator randomStringGenerator = new RandomStringGenerator.Builder()
                .usingRandom(valueRandom::nextInt)
                .build();
        Schema sleeperSchema = new Schema();
        sleeperSchema.setRowKeyFields(
                new Field("key0", sleeperKeyTypeDimension0),
                new Field("key1", sleeperKeyTypeDimension1));
        sleeperSchema.setValueFields(
                new Field("intValue", new IntType()),
                new Field("longValue", new LongType()),
                new Field("stringValue", new StringType()),
                new Field("byteArrayValue", new ByteArrayType()),
                new Field("listOfStringsValue", new ListType(new StringType())),
                new Field("mapFromLongToStringValue", new MapType(new LongType(), new StringType())));
        List<Record> recordList = IntStream.range(0, noOfRecords)
                .mapToObj(i -> {
                    Record record = new Record();
                    record.put(sleeperSchema.getRowKeyFieldNames().get(0), keyObjectsDimension0.get(i));
                    record.put(sleeperSchema.getRowKeyFieldNames().get(1), keyObjectsDimension1.get(i));
                    record.put(sleeperSchema.getValueFieldNames().get(0), valueRandom.nextInt());
                    record.put(sleeperSchema.getValueFieldNames().get(1), valueRandom.nextLong());
                    record.put(sleeperSchema.getValueFieldNames().get(2), randomStringGenerator.generate(valueRandom.nextInt(50)));
                    record.put(sleeperSchema.getValueFieldNames().get(3), randomStringGenerator.generate(valueRandom.nextInt(50)).getBytes(StandardCharsets.UTF_8));
                    record.put(
                            sleeperSchema.getValueFieldNames().get(4),
                            IntStream.range(0, valueRandom.nextInt(10))
                                    .mapToObj(dummy -> randomStringGenerator.generate(valueRandom.nextInt(50)))
                                    .collect(Collectors.toList()));
                    record.put(
                            sleeperSchema.getValueFieldNames().get(5),
                            IntStream.range(0, valueRandom.nextInt(10)).boxed()
                                    .collect(Collectors.toMap(
                                            dummy -> valueRandom.nextLong(),
                                            dummy -> randomStringGenerator.generate(valueRandom.nextInt(50)))));
                    return record;
                }).collect(Collectors.toList());
        Collections.shuffle(recordList, new Random(0));
        return new RecordListAndSchema(recordList, sleeperSchema);
    }

    public static <T, U> RecordListAndSchema genericKey1DSort1D(Type sleeperKeyTypeDimension0,
                                                                Type sleeperSortKeyTypeDimension0,
                                                                List<T> keyObjectsDimension0,
                                                                List<U> sortKeyObjectsDimension0) {
        int noOfRecords = keyObjectsDimension0.size();
        if (sortKeyObjectsDimension0.size() != noOfRecords) {
            throw new AssertionError();
        }
        Random valueRandom = new Random(0);
        RandomStringGenerator randomStringGenerator = new RandomStringGenerator.Builder()
                .usingRandom(valueRandom::nextInt)
                .build();
        Schema sleeperSchema = new Schema();
        sleeperSchema.setRowKeyFields(new Field("key0", sleeperKeyTypeDimension0));
        sleeperSchema.setSortKeyFields(new Field("sortKey0", sleeperSortKeyTypeDimension0));
        sleeperSchema.setValueFields(
                new Field("intValue", new IntType()),
                new Field("longValue", new LongType()),
                new Field("stringValue", new StringType()),
                new Field("byteArrayValue", new ByteArrayType()),
                new Field("listOfStringsValue", new ListType(new StringType())),
                new Field("mapFromLongToStringValue", new MapType(new LongType(), new StringType())));
        List<Record> recordList = IntStream.range(0, noOfRecords)
                .mapToObj(i -> {
                    Record record = new Record();
                    record.put(sleeperSchema.getRowKeyFieldNames().get(0), keyObjectsDimension0.get(i));
                    record.put(sleeperSchema.getSortKeyFieldNames().get(0), sortKeyObjectsDimension0.get(i));
                    record.put(sleeperSchema.getValueFieldNames().get(0), valueRandom.nextInt());
                    record.put(sleeperSchema.getValueFieldNames().get(1), valueRandom.nextLong());
                    record.put(sleeperSchema.getValueFieldNames().get(2), randomStringGenerator.generate(valueRandom.nextInt(50)));
                    record.put(sleeperSchema.getValueFieldNames().get(3), randomStringGenerator.generate(valueRandom.nextInt(50)).getBytes(StandardCharsets.UTF_8));
                    record.put(
                            sleeperSchema.getValueFieldNames().get(4),
                            IntStream.range(0, valueRandom.nextInt(10))
                                    .mapToObj(dummy -> randomStringGenerator.generate(valueRandom.nextInt(50)))
                                    .collect(Collectors.toList()));
                    record.put(
                            sleeperSchema.getValueFieldNames().get(5),
                            IntStream.range(0, valueRandom.nextInt(10)).boxed()
                                    .collect(Collectors.toMap(
                                            dummy -> valueRandom.nextLong(),
                                            dummy -> randomStringGenerator.generate(valueRandom.nextInt(50)))));
                    return record;
                }).collect(Collectors.toList());
        Collections.shuffle(recordList, new Random(0));
        return new RecordListAndSchema(recordList, sleeperSchema);
    }

    public static RecordListAndSchema byteArrayRowKeyLongSortKey(List<byte[]> byteArrayKeys,
                                                                 List<Long> sortKeys,
                                                                 List<Long> values) {
        if (!(byteArrayKeys.size() == sortKeys.size() && sortKeys.size() == values.size())) {
            throw new AssertionError();
        }
        Schema sleeperSchema = new Schema();
        sleeperSchema.setRowKeyFields(new Field("key", new ByteArrayType()));
        sleeperSchema.setSortKeyFields(new Field("sort", new LongType()));
        sleeperSchema.setValueFields(new Field("value", new LongType()));

        List<Record> recordList = IntStream.range(0, byteArrayKeys.size())
                .mapToObj(i -> {
                    Record record = new Record();
                    record.put(sleeperSchema.getRowKeyFieldNames().get(0), byteArrayKeys.get(i));
                    record.put(sleeperSchema.getSortKeyFieldNames().get(0), sortKeys.get(i));
                    record.put(sleeperSchema.getValueFieldNames().get(0), values.get(i));
                    return record;
                }).collect(Collectors.toList());
        Collections.shuffle(recordList, new Random(0));
        return new RecordListAndSchema(recordList, sleeperSchema);
    }

    public static class RecordListAndSchema {
        public List<Record> recordList;
        public Schema sleeperSchema;

        public RecordListAndSchema(List<Record> recordList, Schema sleeperSchema) {
            this.recordList = recordList;
            this.sleeperSchema = sleeperSchema;
        }
    }
}
