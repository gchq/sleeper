/*
 * Copyright 2022-2023 Crown Copyright
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.table.index.DynamoDBTableIndex;
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
import sleeper.core.statestore.StateStoreException;
import sleeper.core.table.TableIdentity;
import sleeper.core.table.TableIndex;
import sleeper.query.model.Query;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

/**
 * Allows a user to enter a query from the command line.
 */
public abstract class QueryCommandLineClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryCommandLineClient.class);

    private final TableIndex tableIndex;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final InstanceProperties instanceProperties;

    protected QueryCommandLineClient(AmazonS3 s3Client, AmazonDynamoDB dynamoDBClient, InstanceProperties instanceProperties) {
        this.instanceProperties = instanceProperties;
        this.tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoDBClient);
        this.tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient);
    }

    public void run() throws StateStoreException {
        TableProperties tableProperties = getTableProperties();
        init(tableProperties);

        runQueries(tableProperties);
    }

    protected abstract void init(TableProperties tableProperties) throws StateStoreException;

    protected abstract void submitQuery(TableProperties tableProperties, Query query);

    protected TableProperties getTableProperties() {
        String tableName = promptTableName();
        if (tableName == null) {
            return null;
        }
        return tablePropertiesProvider.getByName(tableName);
    }

    protected void runQueries(TableProperties tableProperties) {
        String tableName = tableProperties.get(TABLE_NAME);
        Schema schema = tableProperties.getSchema();
        RangeFactory rangeFactory = new RangeFactory(schema);

        Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8.displayName());
        while (true) {
            System.out.print("Exact (e) or range (r) query? ");
            String type = scanner.nextLine();
            if ("".equals(type)) {
                break;
            }
            if (!type.equalsIgnoreCase("e") && !type.equalsIgnoreCase("r")) {
                continue;
            }
            Query query;
            if (type.equalsIgnoreCase("e")) {
                query = constructExactQuery(tableName, schema, rangeFactory, scanner);
            } else {
                query = constructRangeQuery(tableName, schema, rangeFactory, scanner);
            }

            submitQuery(tablePropertiesProvider.getByName(tableName), query);
        }
    }

    private Query constructRangeQuery(String tableName, Schema schema, Range.RangeFactory rangeFactory, Scanner scanner) {
        String entry;
        while (true) {
            System.out.print("Is the minimum inclusive? (y/n) ");
            entry = scanner.nextLine();
            if (entry.equalsIgnoreCase("y") || entry.equalsIgnoreCase("n")) {
                break;
            }
        }
        boolean minInclusive = entry.equalsIgnoreCase("y");
        while (true) {
            System.out.print("Is the maximum inclusive? (y/n) ");
            entry = scanner.nextLine();
            if (entry.equalsIgnoreCase("y") || entry.equalsIgnoreCase("n")) {
                break;
            }
        }
        boolean maxInclusive = entry.equalsIgnoreCase("y");

        List<Range> ranges = new ArrayList<>();
        int i = 0;
        for (Field field : schema.getRowKeyFields()) {
            Object min;
            Object max;
            if (i == 0) {
                System.out.print("Enter a minimum key for row key field " + field.getName() + " of type = " + field.getType() + " - hit return for no minimum: ");
                String minRowKey = scanner.nextLine();
                if ("".equals(minRowKey)) {
                    min = null;
                } else {
                    min = parse(minRowKey, (PrimitiveType) field.getType());
                }
                System.out.print("Enter a maximum key for row key field " + field.getName() + " of type = " + field.getType() + " - hit return for no maximum: ");
                String maxRowKey = scanner.nextLine();
                if ("".equals(maxRowKey)) {
                    max = null;
                } else {
                    max = parse(maxRowKey, (PrimitiveType) field.getType());
                }
            } else {
                while (true) {
                    System.out.print("Enter a value for row key field " + field.getName() + " of type = " + field.getType() + ": (y/n) ");
                    entry = scanner.nextLine();
                    if (entry.equalsIgnoreCase("y") || entry.equalsIgnoreCase("n")) {
                        break;
                    }
                }
                if (entry.equalsIgnoreCase("n")) {
                    break;
                } else {
                    System.out.print("Enter a minimum key for row key field " + field.getName() + " of type = " + field.getType() + " - hit return for no minimum: ");
                    String minRowKey = scanner.nextLine();
                    if ("".equals(minRowKey)) {
                        min = null;
                    } else {
                        min = parse(minRowKey, (PrimitiveType) field.getType());
                    }
                    System.out.print("Enter a maximum key for row key field " + field.getName() + " of type = " + field.getType() + " - hit return for no maximum: ");
                    String maxRowKey = scanner.nextLine();
                    if ("".equals(maxRowKey)) {
                        max = null;
                    } else {
                        max = parse(maxRowKey, (PrimitiveType) field.getType());
                    }
                }
            }
            if (null != min || null != max) {
                if (null == min) {
                    min = getMinimum((PrimitiveType) field.getType());
                }
                Range range = rangeFactory.createRange(field, min, minInclusive, max, maxInclusive);
                ranges.add(range);
            }
            i++;
        }

        Region region = new Region(ranges);

        return Query.builder()
                .tableName(tableName)
                .queryId(UUID.randomUUID().toString())
                .regions(List.of(region))
                .build();
    }

    protected Query constructExactQuery(String tableName, Schema schema, RangeFactory rangeFactory, Scanner scanner) {
        int i = 0;
        List<Range> ranges = new ArrayList<>();
        for (Field field : schema.getRowKeyFields()) {
            String key;
            if (i == 0) {
                System.out.print("Enter a key for row key field " + field.getName() + " of type " + field.getType() + ": ");
                key = scanner.nextLine();
            } else {
                System.out.print("Enter a key for row key field " + field.getName() + " of type " + field.getType() + " - blank for no value: ");
                String entry = scanner.nextLine();
                if ("".equals(entry)) {
                    break;
                } else {
                    key = entry;
                }
            }
            if (null == key) {
                LOGGER.info("Failed to get valid value, restarting creation of exact query");
                return constructExactQuery(tableName, schema, rangeFactory, scanner);
            } else {
                Range range = rangeFactory.createExactRange(field, parse(key, (PrimitiveType) field.getType()));
                ranges.add(range);
            }
            i++;
        }
        Region region = new Region(ranges);
        return Query.builder()
                .tableName(tableName)
                .queryId(UUID.randomUUID().toString())
                .regions(List.of(region))
                .build();
    }

    private String promptTableName() {
        List<String> tables = tableIndex.streamAllTables()
                .map(TableIdentity::getTableName)
                .collect(Collectors.toUnmodifiableList());
        String tableName;
        if (tables.isEmpty()) {
            LOGGER.info("There are no tables. Please create one and add data before running this class.");
            return null;
        }
        if (tables.size() == 1) {
            tableName = tables.get(0);
            LOGGER.info("Querying table " + tableName);
        } else {
            Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8.displayName());
            while (true) {
                LOGGER.info("The system contains the following tables:");
                tables.forEach(System.out::println);
                LOGGER.info("Which table do you wish to query?");
                tableName = scanner.nextLine();
                if (tables.contains(tableName)) {
                    break;
                } else {
                    LOGGER.info("Invalid table, try again");
                }
            }
        }

        LOGGER.info("The table has the schema " + tablePropertiesProvider.getByName(tableName).getSchema());

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
