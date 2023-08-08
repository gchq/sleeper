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

import com.amazonaws.services.s3.AmazonS3;
import org.apache.commons.codec.binary.Base64;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.key.Key;
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
import sleeper.query.model.Query;
import sleeper.table.job.TableLister;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;

import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

/**
 * Allows a user to enter a query from the command line.
 */
public abstract class QueryCommandLineClient {
    private final AmazonS3 s3Client;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final InstanceProperties instanceProperties;

    protected QueryCommandLineClient(AmazonS3 s3Client, InstanceProperties instanceProperties) {
        this.s3Client = s3Client;
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
    }

    public void run() throws StateStoreException {
        TableProperties tableProperties = getTableProperties();
        init(tableProperties);

        runQueries(tableProperties);
    }

    protected abstract void init(TableProperties tableProperties) throws StateStoreException;

    protected abstract void submitQuery(TableProperties tableProperties, Query query);

    protected TableProperties getTableProperties() {
        String tableName = getTableName(s3Client, instanceProperties);
        if (tableName == null) {
            return null;
        }
        return tablePropertiesProvider.getTableProperties(tableName);
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

            submitQuery(tablePropertiesProvider.getTableProperties(tableName), query);
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

        return new Query.Builder(tableName, UUID.randomUUID().toString(), region).build();
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
                System.out.println("Failed to get valid value, restarting creation of exact query");
                return constructExactQuery(tableName, schema, rangeFactory, scanner);
            } else {
                Range range = rangeFactory.createExactRange(field, parse(key, (PrimitiveType) field.getType()));
                ranges.add(range);
            }
            i++;
        }
        Region region = new Region(ranges);
        return new Query.Builder(tableName, UUID.randomUUID().toString(), region).build();
    }

    private String getTableName(AmazonS3 s3Client, InstanceProperties instanceProperties) {
        List<String> tables = new TableLister(s3Client, instanceProperties).listTables();
        String tableName;
        if (tables.isEmpty()) {
            System.out.println("There are no tables. Please create one and add data before running this class.");
            return null;
        }
        if (tables.size() == 1) {
            tableName = tables.get(0);
            System.out.println("Querying table " + tableName);
        } else {
            Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8.displayName());
            while (true) {
                System.out.println("The system contains the following tables:");
                tables.forEach(System.out::println);
                System.out.println("Which table do you wish to query?");
                tableName = scanner.nextLine();
                if (tables.contains(tableName)) {
                    break;
                } else {
                    System.out.println("Invalid table, try again");
                }
            }
        }

        System.out.println("Thie table has schema " + tablePropertiesProvider.getTableProperties(tableName).getSchema());

        return tableName;
    }

    protected InstanceProperties getInstanceProperties() {
        return instanceProperties;
    }

    public Key deserialise(List<String> rowKeys, Schema schema) {
        if (1 == schema.getRowKeyFields().size()) {
            return Key.create(parse(rowKeys.get(0), schema.getRowKeyTypes().get(0)));
        }

        int i = 0;
        List<Object> parsedKeys = new ArrayList<>();
        for (String rowKey : rowKeys) {
            parsedKeys.add(parse(rowKey, schema.getRowKeyTypes().get(i)));
            i++;
        }

        return Key.create(parsedKeys);
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
