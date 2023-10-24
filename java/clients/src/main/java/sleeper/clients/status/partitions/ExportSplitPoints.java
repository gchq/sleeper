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
package sleeper.clients.status.partitions;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.facebook.collections.ByteArray;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;

import sleeper.clients.util.ClientUtils;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.partition.Partition;
import sleeper.core.range.Range;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Allows the split points to be exported from a table. They can then be used
 * to initialise another table with the same partitions (note though that
 * this only exports split points in the first dimension).
 */
public class ExportSplitPoints {
    private final StateStore stateStore;
    private final Schema schema;

    public ExportSplitPoints(StateStore stateStore, Schema schema) {
        this.stateStore = stateStore;
        this.schema = schema;
    }

    public List<Object> getSplitPoints() throws StateStoreException {
        Type rowKey0Type = schema.getRowKeyTypes().get(0);
        List<Partition> leafPartitions = stateStore.getLeafPartitions();
        SortedSet<Comparable<?>> splitPoints = new TreeSet<>();

        for (Partition partition : leafPartitions) {
            Range range = partition.getRegion().getRange(schema.getRowKeyFieldNames().get(0));
            Object min = range.getMin();
            Object max = range.getMax();
            if (rowKey0Type instanceof ByteArrayType) {
                if (null != min) {
                    splitPoints.add(ByteArray.wrap((byte[]) min));
                }
                if (null != max) {
                    splitPoints.add(ByteArray.wrap((byte[]) max));
                }
            } else {
                if (null != min) {
                    splitPoints.add((Comparable) min);
                }
                if (null != max) {
                    splitPoints.add((Comparable) max);
                }
            }
        }

        // Remove minimum value as that is not a split point
        if (rowKey0Type instanceof IntType) {
            splitPoints.remove(Integer.MIN_VALUE);
        } else if (rowKey0Type instanceof LongType) {
            splitPoints.remove(Long.MIN_VALUE);
        } else if (rowKey0Type instanceof StringType) {
            splitPoints.remove("");
        } else if (rowKey0Type instanceof ByteArrayType) {
            splitPoints.remove(ByteArray.wrap(new byte[]{}));
        }

        List<Object> splitPointsToReturn = new ArrayList<>();
        for (Comparable<?> splitPoint : splitPoints) {
            if (rowKey0Type instanceof ByteArrayType) {
                splitPointsToReturn.add(((ByteArray) splitPoint).getArray());
            } else {
                splitPointsToReturn.add(splitPoint);
            }
        }

        return splitPointsToReturn;
    }

    public static void main(String[] args) throws IOException, StateStoreException {
        if (3 != args.length) {
            throw new IllegalArgumentException("Usage: <instance-id> <table-name> <output-file>");
        }

        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();
        InstanceProperties instanceProperties = ClientUtils.getInstanceProperties(amazonS3, args[0]);

        String tableName = args[1];
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, amazonS3, dynamoDBClient);
        TableProperties tableProperties = tablePropertiesProvider.getByName(tableName);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties, new Configuration());
        StateStore stateStore = stateStoreProvider.getStateStore(tableName, tablePropertiesProvider);
        ExportSplitPoints exportSplitPoints = new ExportSplitPoints(stateStore, tableProperties.getSchema());
        List<Object> splitPoints = exportSplitPoints.getSplitPoints();

        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(args[2]), StandardCharsets.UTF_8))) {
            for (Object splitPoint : splitPoints) {
                if (splitPoint instanceof ByteArray) {
                    writer.write(Base64.encodeBase64String(((ByteArray) splitPoint).getArray()));
                } else {
                    writer.write(splitPoint.toString());
                }
                writer.write("\n");
            }
        }

        amazonS3.shutdown();
        dynamoDBClient.shutdown();
    }
}
