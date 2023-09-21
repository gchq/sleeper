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
package sleeper.systemtest.drivers.query;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.QueryLambdaClient;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.key.Key;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.record.Record;
import sleeper.core.record.ResultsBatch;
import sleeper.core.record.serialiser.JSONResultsBatchSerialiser;
import sleeper.core.schema.Schema;
import sleeper.query.model.Query;
import sleeper.systemtest.configuration.SystemTestProperties;
import sleeper.systemtest.datageneration.RandomRecordSupplier;
import sleeper.systemtest.datageneration.RandomRecordSupplierConfig;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.QUERY_RESULTS_QUEUE_URL;

/**
 * Submits random queries to the query queue.
 */
public class MultipleQueries {
    private final long numQueries;
    private final SystemTestProperties systemTestProperties;
    private final AmazonSQS sqsClient;
    private final AmazonS3 s3Client;
    private final AmazonDynamoDB dynamoClient;
    private final String tableName;

    private static final Logger LOGGER = LoggerFactory.getLogger(MultipleQueries.class);

    public MultipleQueries(
            String tableName,
            long numQueries,
            SystemTestProperties systemTestProperties,
            AmazonSQS sqsClient,
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
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(s3Client, systemTestProperties);
        QueryLambdaClient queryLambdaClient = new QueryLambdaClient(s3Client, dynamoClient, sqsClient, systemTestProperties);

        Schema schema = tablePropertiesProvider.getTableProperties(tableName).getSchema();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Supplier<Key> keySupplier = RandomRecordSupplier.getSupplier(schema.getRowKeyTypes(),
                new RandomRecordSupplierConfig(systemTestProperties));
        // Submit queries to queue
        long startTime = System.currentTimeMillis();
        LOGGER.info("Starting run() at {}", LocalDateTime.now());
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
            Query query = new Query.Builder(tableName, UUID.randomUUID().toString(), range).build();
            queryLambdaClient.submitQuery(query);
        }
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        LOGGER.info("Submitted {} queries in {} seconds", numQueries, duration);

        // Poll results queue for query results
        long numQueryResultsReceived = 0L;
        startTime = System.currentTimeMillis();
        while (numQueryResultsReceived < numQueries) {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                    .withQueueUrl(systemTestProperties.get(QUERY_RESULTS_QUEUE_URL))
                    .withMaxNumberOfMessages(10)
                    .withWaitTimeSeconds(20);
            ReceiveMessageResult receiveMessageResult = sqsClient.receiveMessage(receiveMessageRequest);
            System.out.println(receiveMessageResult.getMessages().size() + " messages received");
            for (Message message : receiveMessageResult.getMessages()) {
                numQueryResultsReceived++; // TODO Need to count distinct query ids
                String messageHandle = message.getReceiptHandle();
                String serialisedResults = message.getBody();
                JSONResultsBatchSerialiser serialiser = new JSONResultsBatchSerialiser();
                ResultsBatch resultsBatch = serialiser.deserialise(serialisedResults);
                String queryId = resultsBatch.getQueryId();
                List<Record> records = resultsBatch.getRecords();
                System.out.println(records.size() + " results for query " + queryId);
                totalResults += records.size();
                records.forEach(System.out::println);
                sqsClient.deleteMessage(systemTestProperties.get(QUERY_RESULTS_QUEUE_URL), messageHandle);
            }
        }
        endTime = System.currentTimeMillis();
        duration = (endTime - startTime) / 1000.0;
        double rate = totalResults / duration;
        LOGGER.info("{} records returned in {} seconds at {} per second)", totalResults, duration, String.format("%.2f", rate));
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: <S3 config Bucket> <table name> <Number of queries>");
        }

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        SystemTestProperties systemTestProperties = new SystemTestProperties();
        systemTestProperties.loadFromS3(s3Client, args[0]);
        String tableName = args[1];
        long numQueries = Long.parseLong(args[2]); // TODO Get from system test properties file
        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
        AmazonDynamoDB dynamoClient = AmazonDynamoDBClientBuilder.defaultClient();
        MultipleQueries multipleQueries = new MultipleQueries(tableName, numQueries, systemTestProperties, sqsClient, s3Client, dynamoClient);
        multipleQueries.run();
        s3Client.shutdown();
        sqsClient.shutdown();
    }
}
