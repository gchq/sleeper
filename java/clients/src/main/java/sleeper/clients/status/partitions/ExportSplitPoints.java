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
package sleeper.clients.status.partitions;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.facebook.collections.ByteArray;
import org.apache.hadoop.conf.Configuration;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.range.Range;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.statestore.StateStoreFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static sleeper.configuration.WriteSplitPoints.writeSplitPoints;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

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

    public List<Object> getSplitPoints() {
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
        String instanceId = args[0];
        String tableName = args[1];
        Path outputFile = Paths.get(args[2]);

        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());

        try {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDBClient);
            TableProperties tableProperties = tablePropertiesProvider.getByName(tableName);
            StateStoreFactory stateStoreFactory = new StateStoreFactory(instanceProperties, s3Client, dynamoDBClient, new Configuration());
            StateStore stateStore = stateStoreFactory.getStateStore(tablePropertiesProvider.getByName(tableName));
            ExportSplitPoints exportSplitPoints = new ExportSplitPoints(stateStore, tableProperties.getSchema());
            List<Object> splitPoints = exportSplitPoints.getSplitPoints();

            try (BufferedWriter writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)) {
                writeSplitPoints(splitPoints, writer, false);
            }
        } finally {
            s3Client.shutdown();
            dynamoDBClient.shutdown();
        }
    }
}
