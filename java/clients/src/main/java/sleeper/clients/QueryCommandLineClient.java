/*
 * Copyright 2022-2024 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sleeper.clients;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import org.apache.commons.codec.binary.Base64;

import sleeper.clients.util.console.ConsoleInput;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.configuration.s3properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatus;
import sleeper.query.model.Query;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

/**
 * Allows a user to enter a query from the command line.
 */
public abstract class QueryCommandLineClient {
    private final TableIndex tableIndex;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final InstanceProperties instanceProperties;
    private final Supplier<String> queryIdSupplier;
    protected ConsoleInput in;
    protected ConsoleOutput out;

    protected QueryCommandLineClient(AmazonS3 s3Client, AmazonDynamoDB dynamoDBClient, InstanceProperties instanceProperties) {
        this(s3Client, dynamoDBClient, instanceProperties, new ConsoleInput(System.console()), new ConsoleOutput(System.out));
    }

    protected QueryCommandLineClient(AmazonS3 s3Client, AmazonDynamoDB dynamoDBClient, InstanceProperties instanceProperties,
            ConsoleInput in, ConsoleOutput out) {
        this(instanceProperties, new DynamoDBTableIndex(instanceProperties, dynamoDBClient), S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDBClient), in, out);
    }

    protected QueryCommandLineClient(InstanceProperties instanceProperties, TableIndex tableIndex, TablePropertiesProvider tablePropertiesProvider,
            ConsoleInput in, ConsoleOutput out) {
        this(instanceProperties, tableIndex, tablePropertiesProvider, in, out, () -> UUID.randomUUID().toString());
    }

    protected QueryCommandLineClient(InstanceProperties instanceProperties, TableIndex tableIndex, TablePropertiesProvider tablePropertiesProvider,
            ConsoleInput in, ConsoleOutput out, Supplier<String> queryIdSupplier) {
        this.instanceProperties = instanceProperties;
        this.tableIndex = tableIndex;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.queryIdSupplier = queryIdSupplier;
        this.in = in;
        this.out = out;
    }

    public void run() throws StateStoreException, InterruptedException {
        TableProperties tableProperties = getTableProperties();
        init(tableProperties);

        runQueries(tableProperties);
    }

    protected abstract void init(TableProperties tableProperties) throws StateStoreException;

    protected abstract void submitQuery(TableProperties tableProperties, Query query) throws InterruptedException;

    protected TableProperties getTableProperties() {
        String tableName = promptTableName();
        if (tableName == null) {
            return null;
        }
        return tablePropertiesProvider.getByName(tableName);
    }

    protected void runQueries(TableProperties tableProperties) throws InterruptedException {
        String tableName = tableProperties.get(TABLE_NAME);
        Schema schema = tableProperties.getSchema();
        RangeFactory rangeFactory = new RangeFactory(schema);

        while (true) {
            String type = in.promptLine("Exact (e) or range (r) query? ");
            if ("".equals(type)) {
                break;
            }
            if (!type.equalsIgnoreCase("e") && !type.equalsIgnoreCase("r")) {
                continue;
            }
            Query query;
            if (type.equalsIgnoreCase("e")) {
                query = constructExactQuery(tableName, schema, rangeFactory);
            } else {
                query = constructRangeQuery(tableName, schema, rangeFactory);
            }

            submitQuery(tablePropertiesProvider.getByName(tableName), query);
        }
    }

    private Query constructRangeQuery(String tableName, Schema schema, Range.RangeFactory rangeFactory) {
        boolean minInclusive = promptBoolean("Is the minimum inclusive?");
        boolean maxInclusive = promptBoolean("Is the maximum inclusive?");
        List<Range> ranges = new ArrayList<>();
        int i = 0;
        for (Field field : schema.getRowKeyFields()) {
            String fieldName = field.getName();
            Type fieldType = field.getType();
            if (i > 0 && !promptBoolean("Enter a value for row key field " + fieldName + " of type = " + fieldType + "?")) {
                break;
            }
            Object min = promptForMinKey(fieldName, fieldType);
            Object max = promptForMaxKey(fieldName, fieldType);
            Range range = rangeFactory.createRange(field, min, minInclusive, max, maxInclusive);
            ranges.add(range);
            i++;
        }

        Region region = new Region(ranges);

        return Query.builder()
                .tableName(tableName)
                .queryId(queryIdSupplier.get())
                .regions(List.of(region))
                .build();
    }

    private boolean promptBoolean(String prompt) {
        String entry;
        while (true) {
            entry = in.promptLine(prompt + " (y/n) ");
            if (entry.equalsIgnoreCase("y") || entry.equalsIgnoreCase("n")) {
                return entry.equalsIgnoreCase("y");
            }
        }
    }

    private Object promptForMinKey(String fieldName, Type fieldType) {
        while (true) {
            String minRowKey = in.promptLine("Enter a minimum key for row key field " + fieldName + " of type = " + fieldType + " - hit return for no minimum: ");
            if ("".equals(minRowKey)) {
                return getMinimum((PrimitiveType) fieldType);
            } else {
                try {
                    return parse(minRowKey, (PrimitiveType) fieldType);
                } catch (NumberFormatException e) {
                    out.println("Failed to convert provided key \"" + minRowKey + "\" to type " + fieldType);
                }
            }
        }
    }

    private Object promptForMaxKey(String fieldName, Type fieldType) {
        while (true) {
            String maxRowKey = in.promptLine("Enter a maximum key for row key field " + fieldName + " of type = " + fieldType + " - hit return for no maximum: ");
            if ("".equals(maxRowKey)) {
                return null;
            } else {
                try {
                    return parse(maxRowKey, (PrimitiveType) fieldType);
                } catch (NumberFormatException e) {
                    out.println("Failed to convert provided key \"" + maxRowKey + "\" to type " + fieldType);
                }
            }
        }
    }

    protected Query constructExactQuery(String tableName, Schema schema, RangeFactory rangeFactory) {
        int i = 0;
        List<Range> ranges = new ArrayList<>();
        for (Field field : schema.getRowKeyFields()) {
            String key;
            if (i == 0) {
                key = in.promptLine("Enter a key for row key field " + field.getName() + " of type " + field.getType() + ": ");
            } else {
                String entry = in.promptLine("Enter a key for row key field " + field.getName() + " of type " + field.getType() + " - blank for no value: ");
                if ("".equals(entry)) {
                    break;
                } else {
                    key = entry;
                }
            }
            if (null == key) {
                out.println("Failed to get valid value, restarting creation of exact query");
                return constructExactQuery(tableName, schema, rangeFactory);
            } else {
                Range range = rangeFactory.createExactRange(field, parse(key, (PrimitiveType) field.getType()));
                ranges.add(range);
            }
            i++;
        }
        Region region = new Region(ranges);
        return Query.builder()
                .tableName(tableName)
                .queryId(queryIdSupplier.get())
                .regions(List.of(region))
                .build();
    }

    private String promptTableName() {
        List<String> tables = tableIndex.streamAllTables()
                .map(TableStatus::getTableName)
                .collect(Collectors.toUnmodifiableList());
        String tableName;
        if (tables.isEmpty()) {
            out.println("There are no tables. Please create one and add data before running this class.");
            return null;
        }
        if (tables.size() == 1) {
            tableName = tables.get(0);
            out.println("Querying table " + tableName);
        } else {
            while (true) {
                out.println("The system contains the following tables:");
                tables.forEach(out::println);
                tableName = in.promptLine("Which table do you wish to query?");
                if (tables.contains(tableName)) {
                    break;
                } else {
                    out.println("Invalid table, try again");
                }
            }
        }

        out.println("The table has the schema " + tablePropertiesProvider.getByName(tableName).getSchema());

        return tableName;
    }

    protected InstanceProperties getInstanceProperties() {
        return instanceProperties;
    }

    private Object parse(String string, PrimitiveType type) {
        if ("".equals(string)) {
            return null;
        }
        if (type instanceof IntType) {
            return Integer.parseInt(string);
        }
        if (type instanceof LongType) {
            return Long.parseLong(string);
        }
        if (type instanceof StringType) {
            // TODO Add option for base64 encoded strings
            return string;
        }
        if (type instanceof ByteArrayType) {
            return Base64.decodeBase64(string);
        }
        throw new IllegalArgumentException("Unknown type " + type);
    }

    private Object getMinimum(PrimitiveType type) {
        if (type instanceof IntType) {
            return Integer.MIN_VALUE;
        }
        if (type instanceof LongType) {
            return Long.MIN_VALUE;
        }
        if (type instanceof StringType) {
            return "";
        }
        if (type instanceof ByteArrayType) {
            return new byte[]{};
        }
        throw new IllegalArgumentException("Unknown type: " + type);
    }
}
