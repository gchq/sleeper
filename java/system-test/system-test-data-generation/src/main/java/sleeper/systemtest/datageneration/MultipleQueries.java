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
package sleeper.systemtest.datageneration;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import sleeper.clients.QueryLambdaClient;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.key.Key;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.record.Record;
import sleeper.core.record.ResultsBatch;
import sleeper.core.record.serialiser.JSONResultsBatchSerialiser;
import sleeper.core.schema.Schema;
import sleeper.core.util.LoggedDuration;
import sleeper.query.model.Query;
import sleeper.systemtest.configuration.SystemTestProperties;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import static sleeper.clients.util.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_QUEUE_URL;

/**
 * Submits random queries to the query queue.
 */
public class MultipleQueries {
    private final long numQueries;
    private final SystemTestProperties systemTestProperties;
    private final SqsClient sqsClient;
    private final AmazonS3 s3Client;
    private final AmazonDynamoDB dynamoClient;
    private final String tableName;

    private static final Logger LOGGER = LoggerFactory.getLogger(MultipleQueries.class);

    public MultipleQueries(
            String tableName,
            long numQueries,
            SystemTestProperties systemTestProperties,
            SqsClient sqsClient,
            AmazonS3 s3Client,
            AmazonDynamoDB dynamoClient) {
        this.tableName = tableName;
        this.numQueries = numQueries;
        this.systemTestProperties = systemTestProperties;
        this.sqsClient = sqsClient;
        this.s3Client = s3Client;
        this.dynamoClient = dynamoClient;
    }

    public void run() {
        TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(systemTestProperties, s3Client, dynamoClient);
        QueryLambdaClient queryLambdaClient = new QueryLambdaClient(s3Client, dynamoClient, sqsClient, systemTestProperties);

        Schema schema = tablePropertiesProvider.getByName(tableName).getSchema();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Supplier<Key> keySupplier = RandomRecordSupplier.getSupplier(schema.getRowKeyTypes(),
                new RandomRecordSupplierConfig(systemTestProperties));
        // Submit queries to queue
        Instant startTime = Instant.now();
        LOGGER.info("Starting run() at {}", startTime);
        long totalResults = 0L;
        for (long i = 0L; i < numQueries; i++) {
            Key queryKey = keySupplier.get();
            List<Range> ranges = new ArrayList<>();
            int fieldIndex = 0;
            for (Object object : queryKey.getKeys()) {
                ranges.add(rangeFactory.createExactRange(schema.getRowKeyFields().get(fieldIndex), object));
                fieldIndex++;
            }
            Region range = new Region(ranges);
            Query query = Query.builder()
                    .tableName(tableName)
                    .queryId(UUID.randomUUID().toString())
                    .regions(List.of(range))
                    .build();
            queryLambdaClient.submitQuery(query);
        }
        Instant endTime = Instant.now();
        LOGGER.info("Submitted {} queries in {}", numQueries, LoggedDuration.withFullOutput(startTime, endTime));

        // Poll results queue for query results
        long numQueryResultsReceived = 0L;
        startTime = Instant.now();
        while (numQueryResultsReceived < numQueries) {
            ReceiveMessageResponse response = sqsClient.receiveMessage(request -> request
                    .queueUrl(systemTestProperties.get(QUERY_RESULTS_QUEUE_URL))
                    .maxNumberOfMessages(10)
                    .waitTimeSeconds(20));
            System.out.println(response.messages().size() + " messages received");
            for (Message message : response.messages()) {
                numQueryResultsReceived++; // TODO Need to count distinct query ids
                String messageHandle = message.receiptHandle();
                String serialisedResults = message.body();
                JSONResultsBatchSerialiser serialiser = new JSONResultsBatchSerialiser();
                ResultsBatch resultsBatch = serialiser.deserialise(serialisedResults);
                String queryId = resultsBatch.getQueryId();
                List<Record> records = resultsBatch.getRecords();
                System.out.println(records.size() + " results for query " + queryId);
                totalResults += records.size();
                records.forEach(System.out::println);
                sqsClient.deleteMessage(request -> request
                        .queueUrl(systemTestProperties.get(QUERY_RESULTS_QUEUE_URL))
                        .receiptHandle(messageHandle));
            }
        }
        LoggedDuration duration = LoggedDuration.withFullOutput(startTime, Instant.now());
        double rate = totalResults / (double) duration.getSeconds();
        LOGGER.info("{} records returned in {} at {} per second)", totalResults, duration, String.format("%.2f", rate));
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: <instance id> <table name> <Number of queries>");
        }
        String instanceId = args[0];
        String tableName = args[1];
        long numQueries = Long.parseLong(args[2]); // TODO Get from system test properties file

        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        try (SqsClient sqsClient = buildAwsV2Client(SqsClient.builder())) {
            SystemTestProperties systemTestProperties = SystemTestProperties.loadFromS3GivenInstanceId(s3Client, instanceId);
            MultipleQueries multipleQueries = new MultipleQueries(tableName, numQueries, systemTestProperties, sqsClient, s3Client, dynamoClient);
            multipleQueries.run();
        } finally {
            s3Client.shutdown();
            dynamoClient.shutdown();
        }
    }
}
