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

package sleeper.clients.docker;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorException;
import sleeper.core.iterator.WrappedIterator;
import sleeper.core.record.Record;
import sleeper.core.statestore.StateStoreException;
import sleeper.ingest.IngestFactory;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static sleeper.clients.util.ClientUtils.optionalArgument;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

public class WriteRandomData {
    private WriteRandomData() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1 || args.length > 2) {
            throw new IllegalArgumentException("Usage: <instance-id> <optional-number-of-records>");
        }
        String instanceId = args[0];
        int numberOfRecords = optionalArgument(args, 1).map(Integer::valueOf).orElse(100000);
        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDB = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        InstanceProperties properties = new InstanceProperties();
        properties.loadFromS3GivenInstanceId(s3Client, instanceId);
        TableProperties tableProperties = new TableProperties(properties);
        tableProperties.loadFromS3(s3Client, "system-test");

        writeWithIngestFactory(properties, tableProperties, dynamoDB, numberOfRecords);
    }

    public static void writeWithIngestFactory(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            AmazonDynamoDB dynamoDB, int numberOfRecords) throws IOException {
        writeWithIngestFactory(instanceProperties, tableProperties, dynamoDB, new Configuration(),
                buildS3AsyncClient(S3AsyncClient.builder()), numberOfRecords);
    }

    private static S3AsyncClient buildS3AsyncClient(S3AsyncClientBuilder builder) {
        URI customEndpoint = getCustomEndpoint();
        if (customEndpoint != null) {
            return builder
                    .endpointOverride(customEndpoint)
                    .region(Region.US_EAST_1)
                    .forcePathStyle(true)
                    .build();
        } else {
            return builder.build();
        }
    }

    private static URI getCustomEndpoint() {
        String endpoint = System.getenv("AWS_ENDPOINT_URL");
        if (endpoint != null) {
            return URI.create(endpoint);
        }
        return null;
    }

    public static void writeWithIngestFactory(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            AmazonDynamoDB dynamoDB, Configuration configuration, S3AsyncClient s3AsyncClient, int numberOfRecords)
            throws IOException {
        CloseableIterator<Record> recordIterator = new WrappedIterator<>(
                Stream.generate(getRecordSupplier(10))
                        .limit(numberOfRecords)
                        .iterator());
        IngestFactory ingestFactory = IngestFactory.builder()
                .objectFactory(ObjectFactory.noUserJars())
                .localDir("/tmp/scratch")
                .stateStoreProvider(new StateStoreProvider(dynamoDB, instanceProperties))
                .instanceProperties(instanceProperties)
                .hadoopConfiguration(configuration)
                .s3AsyncClient(s3AsyncClient)
                .build();
        try {
            ingestFactory.ingestFromRecordIteratorAndClose(tableProperties, recordIterator);
        } catch (StateStoreException | IteratorException e) {
            throw new IOException("Failed to write records using iterator", e);
        }
    }

    private static Supplier<Record> getRecordSupplier(int stringLength) {
        return new Supplier<>() {
            private final RandomStringGenerator generator = new RandomStringGenerator.Builder()
                    .withinRange('a', 'z')
                    .build();

            @Override
            public Record get() {
                return new Record(Map.of("key", generator.generate(stringLength)));
            }
        };
    }
}
